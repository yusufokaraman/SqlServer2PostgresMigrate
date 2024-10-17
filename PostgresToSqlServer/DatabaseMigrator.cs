using CsvHelper;
using Npgsql;
using System.Data.SqlClient;
using System.Globalization;
using System.Text;

namespace PostgresToSqlServer
{
    public class DatabaseMigrator
    {
        private Queue<TableDefinition> failedTables = new Queue<TableDefinition>();

        public async Task MigrateDatabaseAsync(NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            try
            {
                var tables = await GetTableDefinitions(pgConn);
                Console.WriteLine($"Toplam Tablo Sayısı: {tables.Count}");

                int tableCount = 0;
                foreach (var table in tables)
                {
                    try
                    {
                        await CreateSchemaIfNotExists(table.Schema, sqlConn);

                        var createTableScript = GenerateCreateTableScript(table);
                        using (var sqlCommand = new SqlCommand(createTableScript, sqlConn))
                        {
                            await sqlCommand.ExecuteNonQueryAsync();
                        }

                        Console.WriteLine($"Tablo oluşturuldu: {table.Schema}.{table.Name}");

                        await BatchMigrateData(table, pgConn, sqlConn);

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

                await RetryFailedTables(pgConn, sqlConn);

                await MigrateForeignKeysAsync(pgConn, sqlConn);

                Console.WriteLine("Yabancı anahtarlar başarıyla aktarıldı.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veritabanı aktarımı sırasında hata: {ex.Message}");
            }
        }

        private async Task<List<TableDefinition>> GetTableDefinitions(NpgsqlConnection pgConn)
        {
            var tables = new List<TableDefinition>();
            var query = @"
        SELECT 
            table_schema,
            table_name,
            column_name,
            data_type,
            is_nullable,
            ordinal_position,
            column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position;
    ";

            using (var command = new NpgsqlCommand(query, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader["table_name"].ToString();
                    var tableSchema = reader["table_schema"].ToString();

                    // PostgreSQL'deki 'public' şemasını SQL Server'da 'dbo' şemasına eşliyoruz
                    if (tableSchema == "public") tableSchema = "dbo";

                    var columnName = reader["column_name"].ToString();
                    var dataType = reader["data_type"].ToString();
                    var isNullable = reader["is_nullable"].ToString() == "YES";

                    var table = tables.FirstOrDefault(t => t.Name == tableName && t.Schema == tableSchema);
                    if (table == null)
                    {
                        table = new TableDefinition { Name = tableName, Schema = tableSchema };
                        tables.Add(table);
                    }

                    table.Columns.Add(new ColumnDefinition
                    {
                        Name = columnName,
                        PgDataType = dataType,
                        IsNullable = isNullable,
                    });
                }
            }

            // Birincil anahtarları almak için ek sorgu
            var pkQuery = @"
        SELECT
            kc.table_schema,
            kc.table_name,
            kc.column_name,
            kc.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kc
            ON kc.table_name = tc.table_name
            AND kc.table_schema = tc.table_schema
            AND kc.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY';
    ";

            using (var command = new NpgsqlCommand(pkQuery, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader["table_name"].ToString();
                    var tableSchema = reader["table_schema"].ToString();

                    // Şema eşleme
                    if (tableSchema == "public") tableSchema = "dbo";

                    var columnName = reader["column_name"].ToString();

                    var table = tables.FirstOrDefault(t => t.Name == tableName && t.Schema == tableSchema);
                    if (table != null)
                    {
                        var column = table.Columns.FirstOrDefault(c => c.Name == columnName);
                        if (column != null)
                        {
                            column.IsPrimaryKey = true;
                            column.PrimaryKeyOrdinal = Convert.ToInt32(reader["ordinal_position"]);
                        }
                    }
                }
            }

            return tables;
        }


        private string GenerateCreateTableScript(TableDefinition table)
        {
            var sb = new StringBuilder();
            //sb.AppendLine($"CREATE SCHEMA IF NOT EXISTS [{table.Schema}];");
            sb.AppendLine($"CREATE TABLE [{table.Schema}].[{table.Name}] (");

            var columnDefinitions = table.Columns.Select(column =>
                $"[{column.Name}] {GetSqlServerDataType(column.PgDataType)} {(column.IsNullable ? "NULL" : "NOT NULL")}");

            var columnsAndConstraints = new List<string>();
            columnsAndConstraints.AddRange(columnDefinitions);

            if (table.Columns.Any(c => c.IsPrimaryKey))
            {
                var primaryKeyColumns = table.Columns
                    .Where(c => c.IsPrimaryKey)
                    .OrderBy(c => c.PrimaryKeyOrdinal)
                    .Select(c => $"[{c.Name}]");
                columnsAndConstraints.Add($"PRIMARY KEY ({string.Join(", ", primaryKeyColumns)})");
            }

            sb.AppendLine(string.Join(",\n", columnsAndConstraints));

            sb.AppendLine(");");
            return sb.ToString();
        }

        private async Task<List<ForeignKeyDefinition>> GetForeignKeyDefinitions(NpgsqlConnection pgConn)
        {
            var foreignKeys = new List<ForeignKeyDefinition>();
            var query = @"
        SELECT
            tc.constraint_name AS constraint_name,
            kcu.table_schema AS fk_schema,
            kcu.table_name AS fk_table,
            kcu.column_name AS fk_column,
            ccu.table_schema AS pk_schema,
            ccu.table_name AS pk_table,
            ccu.column_name AS pk_column
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY';
    ";

            using (var command = new NpgsqlCommand(query, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var fkSchema = reader["fk_schema"].ToString();
                    var pkSchema = reader["pk_schema"].ToString();

                    // Şema eşleme
                    if (fkSchema == "public") fkSchema = "dbo";
                    if (pkSchema == "public") pkSchema = "dbo";

                    foreignKeys.Add(new ForeignKeyDefinition
                    {
                        ConstraintName = reader["constraint_name"].ToString(),
                        ForeignKeySchema = fkSchema,
                        ForeignKeyTable = reader["fk_table"].ToString(),
                        ForeignKeyColumn = reader["fk_column"].ToString(),
                        PrimaryKeySchema = pkSchema,
                        PrimaryKeyTable = reader["pk_table"].ToString(),
                        PrimaryKeyColumn = reader["pk_column"].ToString()
                    });
                }
            }

            return foreignKeys;
        }


        public async Task MigrateForeignKeysAsync(NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            var foreignKeys = await GetForeignKeyDefinitions(pgConn);

            foreach (var fk in foreignKeys)
            {
                try
                {
                    var alterTableScript = $@"
                        ALTER TABLE [{fk.ForeignKeySchema}].[{fk.ForeignKeyTable}]
                        ADD CONSTRAINT [{fk.ConstraintName}]
                        FOREIGN KEY ([{fk.ForeignKeyColumn}])
                        REFERENCES [{fk.PrimaryKeySchema}].[{fk.PrimaryKeyTable}] ([{fk.PrimaryKeyColumn}])
                        ON DELETE CASCADE;
                    ";

                    using (var sqlCommand = new SqlCommand(alterTableScript, sqlConn))
                    {
                        await sqlCommand.ExecuteNonQueryAsync();
                    }

                    Console.WriteLine($"Yabancı anahtar oluşturuldu: {fk.ConstraintName}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Yabancı anahtar oluşturulurken hata oluştu: {fk.ConstraintName}. Hata: {ex.Message}");
                }
            }
        }

        private async Task CreateSchemaIfNotExists(string schemaName, SqlConnection sqlConn)
        {
            if (schemaName == "dbo")
                return;

            var createSchemaCommand = $"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schemaName}') EXEC('CREATE SCHEMA [{schemaName}]')";
            using (var sqlCommand = new SqlCommand(createSchemaCommand, sqlConn))
            {
                await sqlCommand.ExecuteNonQueryAsync();
                Console.WriteLine($"Şema oluşturuldu ya da zaten var: {schemaName}");
            }
        }


        private async Task BatchMigrateData(TableDefinition table, NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            try
            {
                // PostgreSQL'de şema adını kontrol ediyoruz
                var pgSchema = table.Schema == "dbo" ? "public" : table.Schema;
                var pgCommand = new NpgsqlCommand($"SELECT * FROM \"{pgSchema}\".\"{table.Name}\"", pgConn);

                using (var reader = await pgCommand.ExecuteReaderAsync())
                {
                    using (var bulkCopy = new SqlBulkCopy(sqlConn))
                    {
                        bulkCopy.DestinationTableName = $"[{table.Schema}].[{table.Name}]";

                        // Sütun eşlemeleri
                        foreach (var column in table.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                        }

                        await bulkCopy.WriteToServerAsync(reader);
                    }
                }

                Console.WriteLine($"Veriler SqlBulkCopy ile aktarıldı: {table.Schema}.{table.Name}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veri aktarımı sırasında hata oluştu: {ex.Message}");
            }
        }


        private async Task RetryFailedTables(NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            while (failedTables.Count > 0)
            {
                var table = failedTables.Dequeue();
                try
                {
                    Console.WriteLine($"Hatalı tabloyu tekrar aktarıyor: {table.Schema}.{table.Name}");
                    await BatchMigrateData(table, pgConn, sqlConn);
                    Console.WriteLine($"Tablo başarıyla aktarıldı: {table.Schema}.{table.Name}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Tekrar aktarımda hata oluştu: {table.Schema}.{table.Name}. Hata: {ex.Message}");
                }
            }
        }

        private string GetSqlServerDataType(string pgType)
        {
            switch (pgType)
            {
                case "integer":
                    return "INT";
                case "bigint":
                    return "BIGINT";
                case "smallint":
                    return "SMALLINT";
                case "serial":
                    return "INT IDENTITY(1,1)";
                case "bigserial":
                    return "BIGINT IDENTITY(1,1)";
                case "boolean":
                    return "BIT";
                case "timestamp without time zone":
                case "timestamp with time zone":
                case "timestamp":
                    return "DATETIME";
                case "date":
                    return "DATE";
                case "time":
                case "time without time zone":
                case "time with time zone":
                    return "TIME";
                case "text":
                case "character varying":
                case "varchar":
                    return "NVARCHAR(MAX)";
                case "character":
                case "char":
                    return "NVARCHAR(255)";
                case "uuid":
                    return "UNIQUEIDENTIFIER";
                case "bytea":
                    return "VARBINARY(MAX)";
                case "numeric":
                    return "DECIMAL";
                case "real":
                    return "REAL";
                case "double precision":
                    return "FLOAT";
                case "USER-DEFINED":
                case "json":
                case "jsonb":
                case "ARRAY":
                case "hstore":
                case "tsvector":
                    return "NVARCHAR(MAX)";
                default:
                    throw new Exception($"Unsupported PostgreSQL data type: {pgType}");
            }
        }
    }  
}
