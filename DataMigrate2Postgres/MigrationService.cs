using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using DataMigrate2Postgres.AppDbContext;
using Microsoft.EntityFrameworkCore;

namespace DataMigrate2Postgres
{
    public class MigrationService
    {
        private readonly SqlServerContext _sqlServerContext;
        private readonly PostgresContext _postgresContext;

        public MigrationService(SqlServerContext sqlServerContext, PostgresContext postgresContext)
        {
            _sqlServerContext = sqlServerContext;
            _postgresContext = postgresContext;
        }

        public async Task MigrateData()
        {
            Console.WriteLine("Tablo bilgileri alınıyor...");
            var tables = await GetTables();
            if (tables != null)
            {
                Console.WriteLine($"{tables.Count} tablo bulundu.");

                foreach (var table in tables)
                {
                    Console.WriteLine($"Tablo yapısı oluşturuluyor: {table}");
                    var createTableScript = await GetCreateTableScript(table);
                    if (!string.IsNullOrEmpty(createTableScript))
                    {
                        await ExecutePostgresCommand(createTableScript);
                        Console.WriteLine($"{table} tablosu oluşturuldu.");

                        Console.WriteLine($"Tablodan veri alınıyor: {table}");
                        var data = await GetTableData(table);
                        if (data != null)
                        {
                            Console.WriteLine($"{table} tablosundan {data.Count} kayıt alındı.");

                            Console.WriteLine($"{table} tablosuna veri yazılıyor...");
                            await WriteDataToPostgres(table, data);
                            Console.WriteLine($"{table} tablosuna veri yazma işlemi tamamlandı.");
                        }
                        else
                        {
                            Console.WriteLine($"{table} tablosundan veri alınamadı.");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"{table} tablosunun yapısı alınamadı.");
                    }
                }
            }
            else
            {
                Console.WriteLine("Tablo bilgileri alınamadı.");
            }
        }

        private async Task<List<string>> GetTables()
        {
            try
            {
                var tables = new List<string>();
                await using var connection = _sqlServerContext.Database.GetDbConnection();
                await connection.OpenAsync();
                var command = connection.CreateCommand();
                command.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
                var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    tables.Add(reader.GetString(0));
                }
                return tables;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Tablo bilgileri alınırken hata oluştu: {ex.Message}");
                return null;
            }
        }

        private async Task<string> GetCreateTableScript(string tableName)
        {
            try
            {
                await using var connection = _sqlServerContext.Database.GetDbConnection();
                await connection.OpenAsync();
                var command = connection.CreateCommand();
                command.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";
                var reader = await command.ExecuteReaderAsync();
                var createTableScript = $"CREATE TABLE {tableName} (";
                while (await reader.ReadAsync())
                {
                    var columnName = reader.GetString(0);
                    var dataType = reader.GetString(1);
                    var isNullable = reader.GetString(2) == "YES" ? "NULL" : "NOT NULL";
                    var maxLength = reader.IsDBNull(3) ? "" : $"({reader.GetInt32(3)})";

                    createTableScript += $"{columnName} {MapDataType(dataType)}{maxLength} {isNullable}, ";
                }
                createTableScript = createTableScript.TrimEnd(',', ' ') + ");";
                return createTableScript;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Tablo yapısı alınırken hata oluştu ({tableName}): {ex.Message}");
                return null;
            }
        }

        private string MapDataType(string sqlServerDataType)
        {
            return sqlServerDataType switch
            {
                "int" => "INTEGER",
                "bigint" => "BIGINT",
                "smallint" => "SMALLINT",
                "tinyint" => "SMALLINT",
                "bit" => "BOOLEAN",
                "decimal" => "DECIMAL",
                "numeric" => "NUMERIC",
                "money" => "MONEY",
                "smallmoney" => "MONEY",
                "float" => "DOUBLE PRECISION",
                "real" => "REAL",
                "date" => "DATE",
                "datetime" => "TIMESTAMP",
                "datetime2" => "TIMESTAMP",
                "smalldatetime" => "TIMESTAMP",
                "time" => "TIME",
                "char" => "CHAR",
                "varchar" => "VARCHAR",
                "text" => "TEXT",
                "nchar" => "CHAR",
                "nvarchar" => "VARCHAR",
                "ntext" => "TEXT",
                "binary" => "BYTEA",
                "varbinary" => "BYTEA",
                "image" => "BYTEA",
                "uniqueidentifier" => "UUID",
                "xml" => "XML",
                "timestamp" => "BYTEA",
                "rowversion" => "BYTEA",
                _ => throw new NotSupportedException($"Veri tipi desteklenmiyor: {sqlServerDataType}")
            };
        }

        private async Task ExecutePostgresCommand(string commandText)
        {
            try
            {
                await using var connection = _postgresContext.Database.GetDbConnection();
                await connection.OpenAsync();
                var command = connection.CreateCommand();
                command.CommandText = commandText;
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"PostgreSQL komutu çalıştırılırken hata oluştu: {ex.Message}");
            }
        }

        private async Task<List<Dictionary<string, object>>> GetTableData(string tableName)
        {
            try
            {
                var data = new List<Dictionary<string, object>>();
                await using var connection = _sqlServerContext.Database.GetDbConnection();
                await connection.OpenAsync();
                var command = connection.CreateCommand();
                command.CommandText = $"SELECT * FROM {tableName}";
                var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.GetValue(i);
                    }
                    data.Add(row);
                }
                return data;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veri alınırken hata oluştu ({tableName}): {ex.Message}");
                return null;
            }
        }

        private async Task WriteDataToPostgres(string tableName, List<Dictionary<string, object>> data)
        {
            try
            {
                await using var connection = _postgresContext.Database.GetDbConnection();
                await connection.OpenAsync();
                foreach (var row in data)
                {
                    var columns = string.Join(", ", row.Keys);
                    var values = string.Join(", ", row.Values.Select(v => $"'{v}'"));
                    var command = connection.CreateCommand();
                    command.CommandText = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Veri yazılırken hata oluştu ({tableName}): {ex.Message}");
            }
        }
    }
}
