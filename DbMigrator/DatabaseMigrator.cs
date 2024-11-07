using CsvHelper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace DbMigrator
{
    public class DatabaseMigrator
    {
        private Queue<TableDefinition> failedTables = new Queue<TableDefinition>();

        bool isFreshMigration = true;

        public async Task MigrateDatabaseAsync(SqlConnection sqlConn, NpgsqlConnection pgConn)
        {
            try
            {
                await DisableForeignKeys(pgConn);

                var tables = await GetTableDefinitions(sqlConn);
                Console.WriteLine($"Toplam Tablo Sayısı: {tables.Count}");

                int tableCount = 0;
                foreach (var table in tables)
                {
                    try
                    {
                        await EnsureConnectionOpen(pgConn);
                        await CreateSchemaIfNotExists(table.Schema, pgConn);

                        var createTableScript = GenerateCreateTableScript(table);
                        using (var pgCommand = new NpgsqlCommand(createTableScript, pgConn))
                        {
                            await pgCommand.ExecuteNonQueryAsync();
                        }

                        Console.WriteLine($"Tablo oluşturuldu: {table.Schema}.{table.Name}");

                        await BatchMigrateData(table, sqlConn, pgConn, isFreshMigration);

                        tableCount++;
                        Console.WriteLine($"İlerleme: %{((double)tableCount / tables.Count) * 100:0.00}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Tablo aktarılırken hata oluştu: {table.Schema}.{table.Name}. Hata: {ex.Message}");
                        failedTables.Enqueue(table);
                    }
                }

                Console.WriteLine("Veritabanı aktarımı tamamlandı.");

                await RetryFailedTables(sqlConn, pgConn);

                await EnableForeignKeys(sqlConn, pgConn);

                await MigrateForeignKeysAsync(sqlConn, pgConn);

                Console.WriteLine("Yabancı anahtarlar başarıyla aktarıldı.");

                await UpdateSequences(pgConn, tables);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veritabanı aktarımı sırasında hata: {ex.Message}");
            }
        }


        private async Task EnsureConnectionOpen(NpgsqlConnection pgConn)
        {
            if (pgConn.State != System.Data.ConnectionState.Open)
            {
                await pgConn.OpenAsync();
                Console.WriteLine("Bağlantı yeniden açıldı.");
            }
        }

        private async Task<List<TableDefinition>> GetTableDefinitions(SqlConnection sqlConn)
        {
            var tables = new List<TableDefinition>();
            var query = @"
                SELECT 
                    tc.TABLE_NAME, 
                    tc.TABLE_SCHEMA,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
                FROM INFORMATION_SCHEMA.COLUMNS c
                INNER JOIN INFORMATION_SCHEMA.TABLES tc ON c.TABLE_NAME = tc.TABLE_NAME AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
                LEFT JOIN (
                    SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                        ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                        AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                ) pk 
                    ON c.TABLE_CATALOG = pk.TABLE_CATALOG 
                    AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
                    AND c.TABLE_NAME = pk.TABLE_NAME 
                    AND c.COLUMN_NAME = pk.COLUMN_NAME
                WHERE tc.TABLE_TYPE = 'BASE TABLE';
            ";

            using (var command = new SqlCommand(query, sqlConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader["TABLE_NAME"].ToString();
                    var tableSchema = reader["TABLE_SCHEMA"].ToString();
                    var columnName = reader["COLUMN_NAME"].ToString();
                    var dataType = reader["DATA_TYPE"].ToString();
                    var isNullable = reader["IS_NULLABLE"].ToString() == "YES";
                    var isPrimaryKey = Convert.ToBoolean(reader["IS_PRIMARY_KEY"]);

                    var table = tables.FirstOrDefault(t => t.Name == tableName && t.Schema == tableSchema);
                    if (table == null)
                    {
                        table = new TableDefinition { Name = tableName, Schema = tableSchema };
                        tables.Add(table);
                    }

                    table.Columns.Add(new ColumnDefinition
                    {
                        Name = columnName,
                        SqlDataType = dataType,
                        IsNullable = isNullable,
                        IsPrimaryKey = isPrimaryKey
                    });
                }
            }

            return tables;
        }

        private string GenerateCreateTableScript(TableDefinition table)
        {
            var sb = new StringBuilder();
            string schema = table.Schema.ToLower().Replace("ı", "i");
            string tableName = table.Name.ToLower().Replace("ı", "i");

            sb.AppendLine($"CREATE TABLE IF NOT EXISTS \"{schema}\".\"{tableName}\" (");

            var columnDefinitions = table.Columns.Select(column =>
            {
                string columnName = column.Name.ToLower().Replace("ı", "i");
                if (column.IsPrimaryKey && IsAutoIncrementSupported(column.SqlDataType))
                {
                    return $"\"{columnName}\" {GetAutoIncrementColumnDefinition(column.SqlDataType)}";
                }
                return $"\"{columnName}\" {GetPostgreSqlDataType(column.SqlDataType)} {(column.IsNullable ? "NULL" : "NOT NULL")}";
            });

            var columnsAndConstraints = new List<string>();
            columnsAndConstraints.AddRange(columnDefinitions);

            if (table.Columns.Any(c => c.IsPrimaryKey))
            {
                var primaryKeyColumns = table.Columns.Where(c => c.IsPrimaryKey)
                                                     .Select(c => $"\"{c.Name.ToLower().Replace("ı", "i")}\"");
                columnsAndConstraints.Add($"PRIMARY KEY ({string.Join(", ", primaryKeyColumns)})");
            }

            sb.AppendLine(string.Join(",\n", columnsAndConstraints));
            sb.AppendLine(");");

            return sb.ToString();
        }

        private bool IsAutoIncrementSupported(string sqlType)
        {
            return sqlType == "int" || sqlType == "smallint" || sqlType == "bigint";
        }

        private async Task<List<ForeignKeyDefinition>> GetForeignKeyDefinitions(SqlConnection sqlConn)
        {
            var foreignKeys = new List<ForeignKeyDefinition>();
            var query = @"
                    SELECT 
                        fk.CONSTRAINT_NAME,
                        fk.TABLE_SCHEMA AS FK_SCHEMA,
                        fk.TABLE_NAME AS FK_TABLE,
                        kcu.COLUMN_NAME AS FK_COLUMN,
                        pk.TABLE_SCHEMA AS PK_SCHEMA,
                        pk.TABLE_NAME AS PK_TABLE,
                        kcu2.COLUMN_NAME AS PK_COLUMN
                    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
                    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON fk.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2 ON pk.CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME AND kcu.ORDINAL_POSITION = kcu2.ORDINAL_POSITION
                    ORDER BY fk.TABLE_NAME, kcu.COLUMN_NAME;
                ";

            using (var command = new SqlCommand(query, sqlConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    foreignKeys.Add(new ForeignKeyDefinition
                    {
                        ConstraintName = reader["CONSTRAINT_NAME"].ToString(),
                        ForeignKeySchema = reader["FK_SCHEMA"].ToString(),
                        ForeignKeyTable = reader["FK_TABLE"].ToString(),
                        ForeignKeyColumn = reader["FK_COLUMN"].ToString(),
                        PrimaryKeySchema = reader["PK_SCHEMA"].ToString(),
                        PrimaryKeyTable = reader["PK_TABLE"].ToString(),
                        PrimaryKeyColumn = reader["PK_COLUMN"].ToString()
                    });
                }
            }

            return foreignKeys;
        }

        public async Task MigrateForeignKeysAsync(SqlConnection sqlConn, NpgsqlConnection pgConn)
        {
            var foreignKeys = await GetForeignKeyDefinitions(sqlConn);

            var pendingForeignKeys = new Queue<ForeignKeyDefinition>(foreignKeys);
            var failedForeignKeys = new List<ForeignKeyDefinition>();
            var attemptCounts = new Dictionary<string, int>();

            int maxAttempts = 3;

            while (pendingForeignKeys.Count > 0)
            {
                int count = pendingForeignKeys.Count;
                bool progressMade = false;

                for (int i = 0; i < count; i++)
                {
                    var fk = pendingForeignKeys.Dequeue();

                    if (!attemptCounts.ContainsKey(fk.ConstraintName))
                    {
                        attemptCounts[fk.ConstraintName] = 1;
                    }
                    else
                    {
                        attemptCounts[fk.ConstraintName]++;
                    }

                    try
                    {
                        ///Yabancı Anahtar kısıtlamaları başta etkin olmayacak.
                        var alterTableScript = $@"
                            ALTER TABLE ""{fk.ForeignKeySchema}"".""{fk.ForeignKeyTable}""
                            ADD CONSTRAINT ""{fk.ConstraintName}""
                            FOREIGN KEY (""{fk.ForeignKeyColumn}"")
                            REFERENCES ""{fk.PrimaryKeySchema}"".""{fk.PrimaryKeyTable}"" (""{fk.PrimaryKeyColumn}"")
                            ON DELETE CASCADE;
                            DEFERRABLE INITIALLY DEFERRED;
                            ";

                        using (var pgCommand = new NpgsqlCommand(alterTableScript, pgConn))
                        {
                            await pgCommand.ExecuteNonQueryAsync();
                        }

                        Console.WriteLine($"Yabancı anahtar oluşturuldu: {fk.ConstraintName}");
                        progressMade = true;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Yabancı anahtar oluşturulurken hata oluştu: {fk.ConstraintName}. Deneme {attemptCounts[fk.ConstraintName]} / {maxAttempts}. Hata: {ex.Message}");

                        if (attemptCounts[fk.ConstraintName] < maxAttempts)
                        {
                            pendingForeignKeys.Enqueue(fk);
                        }
                        else
                        {
                            failedForeignKeys.Add(fk);
                            Console.WriteLine($"Yabancı anahtar oluşturma denemeleri başarısız oldu: {fk.ConstraintName}");
                        }
                    }
                }

                if (!progressMade)
                    break;
            }

            if (failedForeignKeys.Count > 0)
            {
                Console.WriteLine("Aşağıdaki yabancı anahtarlar oluşturulamadı:");
                foreach (var fk in failedForeignKeys)
                {
                    Console.WriteLine($"- {fk.ConstraintName}");
                }
            }
            else
            {
                Console.WriteLine("Tüm yabancı anahtarlar başarıyla oluşturuldu.");
            }
        }

        private async Task CreateSchemaIfNotExists(string schemaName, NpgsqlConnection pgConn)
        {
            var createSchemaCommand = $"CREATE SCHEMA IF NOT EXISTS \"{schemaName}\"";
            using (var pgCommand = new NpgsqlCommand(createSchemaCommand, pgConn))
            {
                await pgCommand.ExecuteNonQueryAsync();
                Console.WriteLine($"Şema oluşturuldu ya da zaten var: {schemaName}");
            }
        }

        private async Task BatchMigrateData(TableDefinition table, SqlConnection sqlConn, NpgsqlConnection pgConn, bool isFreshMigration)
        {
            var folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CsvExports");
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }

            var tempFile = Path.Combine(folderPath, $"{table.Schema}.{table.Name}.csv");
            List<string> commonColumns;

            if (isFreshMigration)
            {
                // PostgreSQL uyumlu kolon adları (küçük harfe çevirme ve `ı`'yı `i` ile değiştirme)
                commonColumns = table.Columns.Select(c => c.Name.ToLower().Replace("ı", "i")).ToList();
            }
            else
            {
                var postgresColumns = await GetPostgreSqlColumns(table.Schema.ToLower().Replace("ı", "i"), table.Name.ToLower().Replace("ı", "i"), pgConn);
                commonColumns = table.Columns
                    .Where(c => postgresColumns.Contains(c.Name.ToLower().Replace("ı", "i")))
                    .Select(c => c.Name)
                    .ToList();

                if (commonColumns.Count == 0)
                {
                    Console.WriteLine($"Ortak kolon bulunamadı: {table.Schema}.{table.Name}");
                    return;
                }
            }

            try
            {
                using (var writer = new StreamWriter(tempFile, false, Encoding.UTF8))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    foreach (var column in commonColumns)
                    {
                        csv.WriteField(column.ToLower().Replace("ı", "i"));  // PostgreSQL'e uygun hale getir
                    }
                    await csv.NextRecordAsync();

                    var sqlCommand = new SqlCommand($"SELECT {string.Join(",", commonColumns.Select(c => $"[{c}]"))} FROM [{table.Schema}].[{table.Name}]", sqlConn);
                    using (var reader = await sqlCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            foreach (var column in commonColumns)
                            {
                                var value = reader.IsDBNull(reader.GetOrdinal(column)) ? (object)DBNull.Value : reader.GetValue(reader.GetOrdinal(column));
                                csv.WriteField(value ?? string.Empty);
                            }
                            await csv.NextRecordAsync();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"CSV dosyası oluşturulurken hata oluştu: {ex.Message}");
                return;
            }

            try
            {
                var copyCommand = $"COPY \"{table.Schema.ToLower().Replace("ı", "i")}\".\"{table.Name.ToLower().Replace("ı", "i")}\" ({string.Join(",", commonColumns.Select(c => $"\"{c.ToLower().Replace("ı", "i")}\""))}) FROM STDIN (FORMAT csv, HEADER true)";
                using (var pgWriter = pgConn.BeginTextImport(copyCommand))
                using (var reader = new StreamReader(tempFile))
                {
                    string line;
                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        await pgWriter.WriteLineAsync(line);
                    }
                }
                Console.WriteLine($"Veriler COPY komutuyla aktarıldı: {table.Schema}.{table.Name}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"COPY komutuyla aktarım sırasında hata oluştu: {ex.Message}");
                Console.WriteLine($"Hata: {ex}");
            }
        }



        private async Task DisableForeignKeys(NpgsqlConnection pgConn)
        {
            var disableForeignKeysCommand = @"
    DO
    $$
    DECLARE
        tbl RECORD;
    BEGIN
        FOR tbl IN 
            SELECT tc.table_schema, tc.table_name, tc.constraint_name 
            FROM information_schema.table_constraints AS tc
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', tbl.table_schema, tbl.table_name, tbl.constraint_name);
        END LOOP;
    END
    $$;";

            using (var command = new NpgsqlCommand(disableForeignKeysCommand, pgConn))
            {
                await command.ExecuteNonQueryAsync();
                Console.WriteLine("Tüm yabancı anahtarlar devre dışı bırakıldı.");
            }
        }

        private async Task EnableForeignKeys(SqlConnection sqlConn, NpgsqlConnection pgConn)
        {
            var foreignKeys = await GetForeignKeyDefinitions(sqlConn);

            foreach (var fk in foreignKeys)
            {
                try
                {
                    var enableForeignKeyCommand = $@"
                ALTER TABLE ""{fk.ForeignKeySchema}"".""{fk.ForeignKeyTable}""
                ADD CONSTRAINT ""{fk.ConstraintName}""
                FOREIGN KEY (""{fk.ForeignKeyColumn}"")
                REFERENCES ""{fk.PrimaryKeySchema}"".""{fk.PrimaryKeyTable}"" (""{fk.PrimaryKeyColumn}"")
                ON DELETE CASCADE
                DEFERRABLE INITIALLY DEFERRED;";

                    using (var pgCommand = new NpgsqlCommand(enableForeignKeyCommand, pgConn))
                    {
                        await pgCommand.ExecuteNonQueryAsync();
                    }

                    Console.WriteLine($"Yabancı anahtar etkinleştirildi: {fk.ConstraintName}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Yabancı anahtar etkinleştirilirken hata oluştu: {fk.ConstraintName}. Hata: {ex.Message}");
                }
            }

            Console.WriteLine("Tüm yabancı anahtarlar başarıyla etkinleştirildi.");
        }

        private async Task<List<string>> GetPostgreSqlColumns(string schema, string tableName, NpgsqlConnection pgConn)
        {
            var columns = new List<string>();
            var query = $@"
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{tableName}';";

            using (var command = new NpgsqlCommand(query, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    columns.Add(reader.GetString(0));
                }
            }
            return columns;
        }

        private async Task RetryFailedTables(SqlConnection sqlConn, NpgsqlConnection pgConn)
        {
            while (failedTables.Count > 0)
            {
                var table = failedTables.Dequeue();
                try
                {
                    Console.WriteLine($"Hatalı tabloyu tekrar aktarıyor: {table.Schema}.{table.Name}");
                    await EnsureConnectionOpen(pgConn);
                    await BatchMigrateData(table, sqlConn, pgConn, isFreshMigration);
                    Console.WriteLine($"Tablo başarıyla aktarıldı: {table.Schema}.{table.Name}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Tekrar aktarımda hata oluştu: {table.Schema}.{table.Name}. Hata: {ex.Message}");
                }
            }
        }

        private async Task UpdateSequences(NpgsqlConnection pgConn, List<TableDefinition> tables)
        {
            foreach (var table in tables)
            {
                try
                {
                    var primaryKeyColumn = table.Columns.FirstOrDefault(c => c.IsPrimaryKey && IsAutoIncrementSupported(c.SqlDataType));

                    if (primaryKeyColumn == null)
                    {
                        Console.WriteLine($"Tablo {table.Schema}.{table.Name} için otomatik artan Primary Key bulunamadı, sekans güncellenmeyecek.");
                        continue;
                    }

                    var schemaName = table.Schema.ToLower().Replace("ı", "i");
                    var tableName = table.Name.ToLower().Replace("ı", "i");
                    var columnName = primaryKeyColumn.Name.ToLower().Replace("ı", "i");

                    var updateSequenceCommand = $@"
                        SELECT setval(pg_get_serial_sequence('""{schemaName}"".""{tableName}""', '{columnName}'),
                        COALESCE(MAX(""{columnName}""), 1)) 
                        FROM ""{schemaName}"".""{tableName}"";";

                    using (var pgCommand = new NpgsqlCommand(updateSequenceCommand, pgConn))
                    {
                        await pgCommand.ExecuteNonQueryAsync();
                        Console.WriteLine($"Sekans güncellendi: {table.Schema}.{table.Name}.{primaryKeyColumn.Name}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Sekans güncellenirken hata oluştu: {table.Schema}.{table.Name}. Hata: {ex.Message}");
                }
            }
        }


        private string GetAutoIncrementColumnDefinition(string sqlType)
        {
            switch (sqlType)
            {
                case "int":
                case "smallint":
                case "bigint":
                    return $"{GetPostgreSqlDataType(sqlType)} GENERATED ALWAYS AS IDENTITY";
                default:
                    throw new Exception($"Unsupported auto-increment SQL Server data type: {sqlType}");
            }
        }

        private string GetPostgreSqlDataType(string sqlType)
        {
            switch (sqlType)
            {
                case "nvarchar":
                case "varchar":
                case "nchar":
                case "text":
                case "ntext":
                case "hierarchyid":
                    return "text";
                case "char":
                    return "char";
                case "uniqueidentifier":
                    return "uuid";
                case "datetime":
                case "date":
                case "datetime2":
                case "smalldatetime":
                    return "timestamp";
                case "time":
                    return "time";
                case "bit":
                    return "boolean";
                case "decimal":
                case "numeric":
                    return "numeric";
                case "int":
                case "smallint":
                case "tinyint":
                    return "integer";
                case "bigint":
                    return "bigint";
                case "float":
                case "real":
                    return "double precision";
                case "varbinary":
                case "timestamp":
                case "image":
                    return "bytea";
                case "money":
                    return "numeric(19,4)";
                case "smallmoney":
                    return "numeric(10,4)";
                case "xml":
                    return "xml";
                case "geography":
                    return "geography";
                default:
                    throw new Exception($"Unsupported SQL Server data type: {sqlType}");
            }
        }
    }
}