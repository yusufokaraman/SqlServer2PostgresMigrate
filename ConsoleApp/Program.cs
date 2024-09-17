using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using System.IO;

internal class Program
{
    private static void Main(string[] args)
    {
        var logFilePath = "migration_log.txt"; // Log dosyası
        File.WriteAllText(logFilePath, "Migration process started...\n");

        var sqlConnectionString = "Server=10.254.183.242;Database=Emptor_ProbilServis_Prod;User Id=ahmet.bilik;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;";
        var postgresConnectionString = "Host=10.254.183.242;Port=5433;Database=EmptorProbilServisProd;Username=sa;Password=Abc123def!!!;Timeout=500";

        try
        {
            using (var sqlConnection = new SqlConnection(sqlConnectionString))
            using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
            {
                sqlConnection.Open();
                postgresConnection.Open();
                Log(logFilePath, "Connected to both SQL Server and PostgreSQL");

                var tables = sqlConnection.Query("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'").ToList();
                int totalTables = tables.Count;
                Console.WriteLine($"Total number of tables to process: {totalTables}");
                Log(logFilePath, $"Total number of tables to process: {totalTables}");

                int currentTableIndex = 0;

                foreach (var table in tables)
                {
                    try
                    {
                        currentTableIndex++;
                        string schema = table.TABLE_SCHEMA;
                        string tableName = table.TABLE_NAME;

                        // İlerleme yüzdesi
                        double progressPercentage = (double)currentTableIndex / totalTables * 100;
                        Console.WriteLine($"Processing table {currentTableIndex}/{totalTables} ({progressPercentage:F2}% complete): {schema}.{tableName}");
                        Log(logFilePath, $"Processing table {currentTableIndex}/{totalTables} ({progressPercentage:F2}% complete): {schema}.{tableName}");

                        var createSchemaScript = $"CREATE SCHEMA IF NOT EXISTS \"{schema}\"";
                        using (var schemaCommand = new NpgsqlCommand(createSchemaScript, postgresConnection))
                        {
                            schemaCommand.ExecuteNonQuery();
                        }

                        string checkTableExistsScript = $@"
                            SELECT EXISTS (
                                SELECT 1 
                                FROM   information_schema.tables 
                                WHERE  table_schema = '{schema}' 
                                AND    table_name = '{tableName}'
                            );";

                        using (var checkTableCommand = new NpgsqlCommand(checkTableExistsScript, postgresConnection))
                        {
                            bool tableExists = (bool)checkTableCommand.ExecuteScalar();

                            if (!tableExists)
                            {
                                var columns = sqlConnection.Query($@"
                                    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'");

                                string createTableScript = $"CREATE TABLE \"{schema}\".\"{tableName}\" (";

                                foreach (var column in columns)
                                {
                                    string columnName = column.COLUMN_NAME;
                                    string dataType = ConvertSqlTypeToPostgres(column.DATA_TYPE, column.NUMERIC_PRECISION, column.NUMERIC_SCALE);

                                    createTableScript += $"\"{columnName}\" {dataType},";
                                }

                                createTableScript = createTableScript.TrimEnd(',') + ");";

                                Console.WriteLine($"Create Table Script for {tableName}: {createTableScript}");
                                Log(logFilePath, $"Create Table Script for {tableName}: {createTableScript}");

                                using (var command = new NpgsqlCommand(createTableScript, postgresConnection))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }
                        }

                        // Veri yoksa INSERT işlemini atla
                        var data = sqlConnection.Query($"SELECT * FROM \"{schema}\".\"{tableName}\"").ToList();

                        if (data.Count == 0)
                        {
                            Console.WriteLine($"Table {schema}.{tableName} has no data, skipping insert.");
                            Log(logFilePath, $"Table {schema}.{tableName} has no data, skipping insert.");
                            continue;  // Verisi olmayan tabloyu atla
                        }

                        var columnsForInsert = sqlConnection.Query($@"
                            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'");

                        using (var transaction = postgresConnection.BeginTransaction())
                        {
                            const int batchSize = 1000; // Her 1000 kayıtta bir gönderim yap
                            int rowCount = 0;
                            var valueSets = new List<string>();

                            foreach (var row in data)
                            {
                                var values = new List<string>();
                                foreach (var column in columnsForInsert)
                                {
                                    var value = ((IDictionary<string, object>)row)[column.COLUMN_NAME];
                                    if (value == null || Convert.IsDBNull(value))
                                        values.Add("NULL");
                                    else if (value is DateTime)
                                        values.Add($"'{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss")}'");
                                    else if (value is decimal || value is double || value is float)
                                        values.Add(value.ToString().Replace(",", "."));  // Virgülleri nokta ile değiştir
                                    else
                                        values.Add($"'{value.ToString().Replace("'", "''")}'");
                                }
                                valueSets.Add($"({string.Join(",", values)})");

                                rowCount++;

                                // Her batchSize kadar bir ekleme yap
                                if (rowCount % batchSize == 0)
                                {
                                    string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" ({string.Join(",", columnsForInsert.Select(c => $"\"{c.COLUMN_NAME}\""))}) VALUES {string.Join(",", valueSets)};";
                                    using (var command = new NpgsqlCommand(insertScript, postgresConnection, transaction))
                                    {
                                        command.ExecuteNonQuery();
                                    }

                                    // İşlem sonrasında listeyi temizle
                                    valueSets.Clear();
                                }
                            }

                            // Kalan veriler varsa son bir kez daha gönder
                            if (valueSets.Any())
                            {
                                string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" ({string.Join(",", columnsForInsert.Select(c => $"\"{c.COLUMN_NAME}\""))}) VALUES {string.Join(",", valueSets)};";
                                using (var command = new NpgsqlCommand(insertScript, postgresConnection, transaction))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }

                            transaction.Commit();
                            Console.WriteLine($"Table {schema}.{tableName} processed successfully.");
                            Log(logFilePath, $"Table {schema}.{tableName} processed successfully.");
                        }
                    }
                    catch (Exception tableEx)
                    {
                        Console.WriteLine($"Error processing table {table.TABLE_NAME}: {tableEx.Message}");
                        Log(logFilePath, $"Error processing table {table.TABLE_NAME}: {tableEx}");
                    }
                }
                Console.WriteLine("All tables processed successfully.");
                Log(logFilePath, "All tables processed successfully.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"General error: {ex.Message}");
            Log(logFilePath, $"General error: {ex}");
        }
    }

    static string ConvertSqlTypeToPostgres(string sqlType, object numericPrecision, object numericScale)
    {
        switch (sqlType.ToLower())
        {
            case "nvarchar":
            case "varchar":
            case "nchar":
            case "text":
            case "ntext":
                return "text";
            case "char":
                return "char";  // veya 'character'
            case "uniqueidentifier":
                return "uuid";
            case "datetime":
            case "date":
            case "datetime2":
                return "timestamp";
            case "bit":
                return "boolean";
            case "decimal":
            case "numeric":
                if (numericPrecision != null && numericScale != null)
                    return $"numeric({numericPrecision}, {numericScale})";
                else
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
            case "rowstamp":
            case "timestamp":
                return "bytea";
            case "money":
                return "numeric(19,4)";
            case "xml":
                return "xml";
            default:
                throw new NotSupportedException($"Unsupported SQL type: {sqlType}");
        }
    }

    static void Log(string filePath, string message)
    {
        File.AppendAllText(filePath, $"{DateTime.Now}: {message}\n");
    }
}
