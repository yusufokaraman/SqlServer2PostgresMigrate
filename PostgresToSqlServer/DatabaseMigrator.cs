using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                        // "public" şemasını "dbo" olarak değiştir
                        if (table.Schema == "public")
                            table.Schema = "dbo";

                        await EnsureConnectionOpen(sqlConn);  // Bağlantıyı kontrol et
                        await CreateSchemaIfNotExists(table.Schema, sqlConn);  // Şemayı oluştur

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

                // Tablolar arası ilişkileri aktar
                await MigrateConstraints(pgConn, sqlConn);

                await RetryFailedTables(pgConn, sqlConn);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veritabanı aktarımı sırasında hata: {ex.Message}");
            }
        }

        private async Task EnsureConnectionOpen(SqlConnection sqlConn)
        {
            if (sqlConn.State != System.Data.ConnectionState.Open)
            {
                await sqlConn.OpenAsync();
                Console.WriteLine("Bağlantı yeniden açıldı.");
            }
        }

        private async Task<List<TableDefinition>> GetTableDefinitions(NpgsqlConnection pgConn)
        {
            var tables = new List<TableDefinition>();
            var query = @"
                SELECT 
                    table_name, 
                    table_schema, 
                    column_name, 
                    data_type, 
                    is_nullable,
                    ordinal_position
                FROM 
                    information_schema.columns 
                WHERE 
                    table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY 
                    table_schema, 
                    table_name, 
                    ordinal_position";

            using (var command = new NpgsqlCommand(query, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableName = reader["table_name"].ToString();
                    var tableSchema = reader["table_schema"].ToString();
                    var columnName = reader["column_name"].ToString();
                    var dataType = reader["data_type"].ToString();
                    var isNullable = reader["is_nullable"].ToString() == "YES";

                    var table = tables.Find(t => t.Name == tableName && t.Schema == tableSchema);
                    if (table == null)
                    {
                        table = new TableDefinition { Name = tableName, Schema = tableSchema };
                        tables.Add(table);
                    }
                    table.Columns.Add(new ColumnDefinition { Name = columnName, SqlDataType = dataType, IsNullable = isNullable });
                }
            }

            return tables;
        }

        private string GenerateCreateTableScript(TableDefinition table)
        {
            var sb = new StringBuilder();

            sb.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{table.Schema}')");
            sb.AppendLine($"EXEC('CREATE SCHEMA [{table.Schema}]');");

            sb.AppendLine($"IF OBJECT_ID('[{table.Schema}].[{table.Name}]', 'U') IS NULL");
            sb.AppendLine($"CREATE TABLE [{table.Schema}].[{table.Name}] (");

            for (int i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                var isLastColumn = i == table.Columns.Count - 1;
                sb.Append($"[{column.Name}] {GetSqlServerDataType(column.SqlDataType)} {(column.IsNullable ? "NULL" : "NOT NULL")}");
                if (!isLastColumn)
                    sb.AppendLine(",");
                else
                    sb.AppendLine();
            }

            sb.AppendLine(");");
            return sb.ToString();
        }

        private async Task CreateSchemaIfNotExists(string schemaName, SqlConnection sqlConn)
        {
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
                var query = $"SELECT * FROM \"{(table.Schema == "dbo" ? "public" : table.Schema)}\".\"{table.Name}\"";
                var pgCommand = new NpgsqlCommand(query, pgConn);

                using (var reader = await pgCommand.ExecuteReaderAsync())
                {
                    var dataTable = new System.Data.DataTable();

                    // Verileri DataTable'a yükle
                    dataTable.Load(reader);

                    // DataTable'daki sütun veri tiplerini SQL Server ile uyumlu hale getir
                    foreach (System.Data.DataColumn column in dataTable.Columns)
                    {
                        var columnDef = table.Columns.First(c => c.Name == column.ColumnName);
                        var sqlServerDataType = GetSqlServerDataType(columnDef.SqlDataType);
                        var netType = GetDotNetType(sqlServerDataType);

                        if (column.DataType != netType)
                        {
                            column.DataType = netType;
                        }
                    }

                    using (var bulkCopy = new SqlBulkCopy(sqlConn))
                    {
                        bulkCopy.DestinationTableName = $"[{table.Schema}].[{table.Name}]";

                        // Sütun eşlemelerini ayarla
                        foreach (System.Data.DataColumn column in dataTable.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                        }

                        await bulkCopy.WriteToServerAsync(dataTable);
                    }
                }
                Console.WriteLine($"Veriler başarıyla aktarıldı: {table.Schema}.{table.Name}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veri aktarımı sırasında hata oluştu: {ex.Message}");
                throw;
            }
        }

        private Type GetDotNetType(string sqlServerDataType)
        {
            switch (sqlServerDataType)
            {
                case "int":
                    return typeof(int);
                case "bigint":
                    return typeof(long);
                case "smallint":
                    return typeof(short);
                case "bit":
                    return typeof(bool);
                case "decimal":
                case "numeric":
                    return typeof(decimal);
                case "float":
                    return typeof(double);
                case "real":
                    return typeof(float);
                case "datetime":
                case "date":
                case "time":
                    return typeof(DateTime);
                case "uniqueidentifier":
                    return typeof(Guid);
                case "nvarchar(max)":
                case "nvarchar":
                case "nchar":
                case "varchar":
                case "char":
                case "text":
                    return typeof(string);
                case "varbinary":
                case "varbinary(max)":
                    return typeof(byte[]);
                default:
                    return typeof(string); // Varsayılan olarak string al
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
                    await EnsureConnectionOpen(sqlConn);
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
                case "character varying":
                case "varchar":
                case "text":
                    return "nvarchar(max)";
                case "character":
                case "char":
                    return "nchar(1)";
                case "uuid":
                    return "uniqueidentifier";
                case "timestamp without time zone":
                case "timestamp with time zone":
                case "timestamp":
                    return "datetime";
                case "date":
                    return "date";
                case "time without time zone":
                case "time with time zone":
                case "time":
                    return "time";
                case "boolean":
                    return "bit";
                case "numeric":
                    return "decimal(18,4)";
                case "decimal":
                    return "decimal(18,4)";
                case "integer":
                    return "int";
                case "bigint":
                    return "bigint";
                case "double precision":
                    return "float";
                case "real":
                    return "real";
                case "smallint":
                    return "smallint";
                case "bytea":
                    return "varbinary(max)";
                default:
                    return "nvarchar(max)"; // Varsayılan olarak nvarchar(max)
            }
        }


        private async Task MigrateConstraints(NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            // Önce tüm tabloların birincil anahtarlarını aktaralım
            await MigratePrimaryKeys(pgConn, sqlConn);

            // Ardından yabancı anahtarları aktaralım
            await MigrateForeignKeys(pgConn, sqlConn);
        }

        private async Task MigratePrimaryKeys(NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            var query = @"
                SELECT
                    kcu.table_schema,
                    kcu.table_name,
                    tco.constraint_name,
                    kcu.column_name
                FROM
                    information_schema.table_constraints tco
                JOIN information_schema.key_column_usage kcu 
                    ON kcu.constraint_name = tco.constraint_name
                    AND kcu.constraint_schema = tco.constraint_schema
                    AND kcu.constraint_name = tco.constraint_name
                WHERE tco.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.table_schema, kcu.table_name, tco.constraint_name, kcu.ordinal_position";

            var primaryKeys = new Dictionary<string, List<string>>();

            using (var command = new NpgsqlCommand(query, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var tableSchema = reader["table_schema"].ToString();
                    var tableName = reader["table_name"].ToString();
                    var constraintName = reader["constraint_name"].ToString();
                    var columnName = reader["column_name"].ToString();

                    // "public" şemasını "dbo" olarak değiştir
                    if (tableSchema == "public")
                        tableSchema = "dbo";

                    var key = $"{tableSchema}.{tableName}.{constraintName}";
                    if (!primaryKeys.ContainsKey(key))
                    {
                        primaryKeys[key] = new List<string>();
                    }
                    primaryKeys[key].Add(columnName);
                }
            }

            foreach (var pk in primaryKeys)
            {
                var parts = pk.Key.Split('.');
                var schema = parts[0];
                var table = parts[1];
                var constraint = parts[2];

                var columns = pk.Value.Select(c => $"[{c}]");
                var constraintScript = $@"
                    ALTER TABLE [{schema}].[{table}]
                    ADD CONSTRAINT [{constraint}]
                    PRIMARY KEY ({string.Join(", ", columns)});";

                using (var command = new SqlCommand(constraintScript, sqlConn))
                {
                    await command.ExecuteNonQueryAsync();
                    Console.WriteLine($"Birincil anahtar oluşturuldu: {schema}.{table} - {constraint}");
                }
            }
        }

        private async Task MigrateForeignKeys(NpgsqlConnection pgConn, SqlConnection sqlConn)
        {
            var query = @"
                SELECT
                    tc.constraint_name,
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM 
                    information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'";

            using (var command = new NpgsqlCommand(query, pgConn))
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var constraintName = reader["constraint_name"].ToString();
                    var tableSchema = reader["table_schema"].ToString();
                    var tableName = reader["table_name"].ToString();
                    var columnName = reader["column_name"].ToString();
                    var foreignTableSchema = reader["foreign_table_schema"].ToString();
                    var foreignTableName = reader["foreign_table_name"].ToString();
                    var foreignColumnName = reader["foreign_column_name"].ToString();

                    // "public" şemasını "dbo" olarak değiştir
                    if (tableSchema == "public")
                        tableSchema = "dbo";
                    if (foreignTableSchema == "public")
                        foreignTableSchema = "dbo";

                    var constraintScript = $@"
                        ALTER TABLE [{tableSchema}].[{tableName}]
                        ADD CONSTRAINT [{constraintName}]
                        FOREIGN KEY ([{columnName}])
                        REFERENCES [{foreignTableSchema}].[{foreignTableName}]([{foreignColumnName}]);";

                    using (var sqlCommand = new SqlCommand(constraintScript, sqlConn))
                    {
                        try
                        {
                            await sqlCommand.ExecuteNonQueryAsync();
                            Console.WriteLine($"Yabancı anahtar oluşturuldu: {constraintName}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Yabancı anahtar oluşturulurken hata oluştu: {constraintName}. Hata: {ex.Message}");
                        }
                    }
                }
            }
        }
    }
}
