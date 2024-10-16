using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                await RetryFailedTables(sqlConn, pgConn);
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
            var query = "SELECT TABLE_NAME, TABLE_SCHEMA, COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS";

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

                    var table = tables.Find(t => t.Name == tableName && t.Schema == tableSchema) ?? new TableDefinition { Name = tableName, Schema = tableSchema };
                    table.Columns.Add(new ColumnDefinition { Name = columnName, SqlDataType = dataType, IsNullable = isNullable });

                    if (!tables.Contains(table))
                        tables.Add(table);
                }
            }

            return tables;
        }

        private string GenerateCreateTableScript(TableDefinition table)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE IF NOT EXISTS \"{table.Schema}\".\"{table.Name}\" (");

            foreach (var column in table.Columns)
            {
                var isLastColumn = column == table.Columns.Last();
                sb.AppendLine($"\"{column.Name}\" {GetPostgreSqlDataType(column.SqlDataType)} {(column.IsNullable ? "NULL" : "NOT NULL")}{(isLastColumn ? "" : ",")}");
            }

            sb.AppendLine(");");
            return sb.ToString();
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
                using (var writer = new StreamWriter(tempFile))
                {
                    var sqlCommand = new SqlCommand($"SELECT * FROM [{table.Schema}].[{table.Name}]", sqlConn);
                    using (var reader = await sqlCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var values = new List<string>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var value = reader.IsDBNull(i) ? "NULL" :
                                     (reader.GetFieldType(i) == typeof(DateTime) ? $"'{((DateTime)reader.GetValue(i)).ToString("yyyy-MM-dd HH:mm:ss")}'" :
                                     (reader.GetFieldType(i) == typeof(decimal) || reader.GetFieldType(i) == typeof(double) || reader.GetFieldType(i) == typeof(float) ?
                                     reader.GetValue(i).ToString().Replace(',', '.') :
                                     $"'{reader.GetValue(i).ToString().Replace("'", "''")}'"));
                                values.Add(value);
                            }
                            writer.WriteLine(string.Join(",", values));
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
                var copyCommand = $"COPY \"{table.Schema}\".\"{table.Name}\" FROM '{tempFile}' DELIMITER ',' CSV HEADER";
                using (var pgCommand = new NpgsqlCommand(copyCommand, pgConn))
                {
                    await pgCommand.ExecuteNonQueryAsync();
                    Console.WriteLine($"Veriler COPY komutuyla aktarıldı: {table.Schema}.{table.Name}");
                }
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
                case "xml":
                    return "xml";
                default:
                    throw new Exception($"Unsupported SQL Server data type: {sqlType}");
            }
        }
    }
}
