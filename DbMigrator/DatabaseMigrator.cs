using CsvHelper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Globalization;
using System.Text;

namespace DbMigrator
{
    public class DatabaseMigrator
    {
        private Queue<TableDefinition> failedTables = new Queue<TableDefinition>();

        public async Task MigrateDatabaseAsync(SqlConnection sqlConn, NpgsqlConnection pgConn)
        {
            try
            {
                var tables = await GetTableDefinitions(sqlConn);
                Console.WriteLine($"Toplam Tablo Sayısı: {tables.Count}");

                int tableCount = 0;
                foreach (var table in tables)
                {
                    try
                    {
                        await EnsureConnectionOpen(pgConn);  // Bağlantıyı kontrol et
                        await CreateSchemaIfNotExists(table.Schema, pgConn);  // Şemayı oluştur

                        var createTableScript = GenerateCreateTableScript(table);
                        using (var pgCommand = new NpgsqlCommand(createTableScript, pgConn))
                        {
                            await pgCommand.ExecuteNonQueryAsync();
                        }

                        Console.WriteLine($"Tablo oluşturuldu: {table.Schema}.{table.Name}");

                        await BatchMigrateData(table, sqlConn, pgConn);

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

                // Hatalı tabloları tekrar dene
                await RetryFailedTables(sqlConn, pgConn);

                // Yabancı anahtarları aktarma
                await MigrateForeignKeysAsync(sqlConn, pgConn);

                Console.WriteLine("Yabancı anahtarlar başarıyla aktarıldı.");
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
            sb.AppendLine($"CREATE TABLE IF NOT EXISTS \"{table.Schema}\".\"{table.Name}\" (");

            var columnDefinitions = table.Columns.Select(column =>
                $"\"{column.Name}\" {GetPostgreSqlDataType(column.SqlDataType)} {(column.IsNullable ? "NULL" : "NOT NULL")}");

            var columnsAndConstraints = new List<string>();
            columnsAndConstraints.AddRange(columnDefinitions);

            if (table.Columns.Any(c => c.IsPrimaryKey))
            {
                var primaryKeyColumns = table.Columns.Where(c => c.IsPrimaryKey).Select(c => $"\"{c.Name}\"");
                columnsAndConstraints.Add($"PRIMARY KEY ({string.Join(", ", primaryKeyColumns)})");
            }

            sb.AppendLine(string.Join(",\n", columnsAndConstraints));

            sb.AppendLine(");");
            return sb.ToString();
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

            foreach (var fk in foreignKeys)
            {
                try
                {
                    var alterTableScript = $@"
                ALTER TABLE ""{fk.ForeignKeySchema}"".""{fk.ForeignKeyTable}""
                ADD CONSTRAINT ""{fk.ConstraintName}""
                FOREIGN KEY (""{fk.ForeignKeyColumn}"")
                REFERENCES ""{fk.PrimaryKeySchema}"".""{fk.PrimaryKeyTable}"" (""{fk.PrimaryKeyColumn}"")
                ON DELETE CASCADE;
            ";

                    using (var pgCommand = new NpgsqlCommand(alterTableScript, pgConn))
                    {
                        await pgCommand.ExecuteNonQueryAsync();
                    }

                    Console.WriteLine($"Yabancı anahtar oluşturuldu: {fk.ConstraintName}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Yabancı anahtar oluşturulurken hata oluştu: {fk.ConstraintName}. Hata: {ex.Message}");
                }
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

        private async Task BatchMigrateData(TableDefinition table, SqlConnection sqlConn, NpgsqlConnection pgConn)
        {
            var folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CsvExports");
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }

            var tempFile = Path.Combine(folderPath, $"{table.Schema}.{table.Name}.csv");

            // SQL Server'dan CSV dosyasına veri aktar
            try
            {
                using (var writer = new StreamWriter(tempFile, false, Encoding.UTF8))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    var sqlCommand = new SqlCommand($"SELECT * FROM [{table.Schema}].[{table.Name}]", sqlConn);
                    using (var reader = await sqlCommand.ExecuteReaderAsync())
                    {
                        // Sütun başlıklarını yaz
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            csv.WriteField(reader.GetName(i));
                        }
                        await csv.NextRecordAsync();

                        while (await reader.ReadAsync())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (reader.IsDBNull(i))
                                {
                                    csv.WriteField(string.Empty);
                                }
                                else
                                {
                                    csv.WriteField(reader.GetValue(i));
                                }
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

            // PostgreSQL'e COPY komutuyla CSV dosyasından veri aktar
            try
            {
                var copyCommand = $"COPY \"{table.Schema}\".\"{table.Name}\" FROM STDIN (FORMAT csv, HEADER true)";
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
            }

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
                    await BatchMigrateData(table, sqlConn, pgConn);
                    Console.WriteLine($"Tablo başarıyla aktarıldı: {table.Schema}.{table.Name}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Tekrar aktarımda hata oluştu: {table.Schema}.{table.Name}. Hata: {ex.Message}");
                }
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
