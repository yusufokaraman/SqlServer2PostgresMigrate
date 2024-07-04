using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System;
using DataMigrate2Postgres.AppDbContext;

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
            var connection = _sqlServerContext.Database.GetDbConnection();
            await connection.OpenAsync();
            var command = connection.CreateCommand();
            command.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
            var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(reader.GetString(0));
            }
            await connection.CloseAsync();
            return tables;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Tablo bilgileri alınırken hata oluştu: {ex.Message}");
            return null;
        }
    }

    private async Task<List<Dictionary<string, object>>> GetTableData(string tableName)
    {
        try
        {
            var data = new List<Dictionary<string, object>>();
            var connection = _sqlServerContext.Database.GetDbConnection();
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
            await connection.CloseAsync();
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
            var connection = _postgresContext.Database.GetDbConnection();
            await connection.OpenAsync();
            foreach (var row in data)
            {
                var columns = string.Join(", ", row.Keys);
                var values = string.Join(", ", row.Values.Select(v => $"'{v}'"));
                var command = connection.CreateCommand();
                command.CommandText = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
                await command.ExecuteNonQueryAsync();
            }
            await connection.CloseAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Veri yazılırken hata oluştu ({tableName}): {ex.Message}");
        }
    }
}
