using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

internal class Program
{
    private static readonly object fileLock = new object();
    private static readonly int maxRetryCount = 3; // Retry limit for failed operations

    public static async Task Main(string[] args)
    {
        var logFilePath = "migration_log.txt";
        File.WriteAllText(logFilePath, "Migration process started...\n");

        var sqlConnectionString = "Server=10.254.183.242;Database=Emptor_ProbilServis_Prod;User Id=ahmet.bilik;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        var postgresConnectionString = "Host=localhost;Port=5432;Database=Emptor_ProbilServis_Prod;Username=postgres;Password=Abc123def!!!;Timeout=500";

        List<(string schema, string tableName)> errorTablesQueue = new List<(string schema, string tableName)>();
        List<(string schema, string tableName)> errorTransferQueue = new List<(string schema, string tableName)>();

        try
        {
            using (var sqlConnection = new SqlConnection(sqlConnectionString))
            {
                await sqlConnection.OpenAsync();
                var tables = sqlConnection.Query<(string schema, string tableName)>("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'").ToList();
                int totalTables = tables.Count;
                Log(logFilePath, $"Total number of tables to process: {totalTables}");

                // 1. Create all tables
                Log(logFilePath, "Starting table creation...");
                await CreateTables(tables, sqlConnectionString, postgresConnectionString, logFilePath, errorTablesQueue, totalTables);
                Log(logFilePath, "Table creation completed.");

                // 2. Transfer data after all tables are created
                Log(logFilePath, "Starting data transfer...");
                await TransferData(tables, sqlConnectionString, postgresConnectionString, logFilePath, errorTransferQueue, totalTables);
                Log(logFilePath, "Data transfer completed.");

                // Retry failed operations
                if (errorTablesQueue.Any())
                {
                    Log(logFilePath, "Retrying failed table creations...");
                    await RetryCreateTables(errorTablesQueue, sqlConnectionString, postgresConnectionString, logFilePath);
                }

                if (errorTransferQueue.Any())
                {
                    Log(logFilePath, "Retrying failed data transfers...");
                    await RetryTransferData(errorTransferQueue, sqlConnectionString, postgresConnectionString, logFilePath);
                }

                Log(logFilePath, "Migration process completed.");
            }
        }
        catch (Exception ex)
        {
            Log(logFilePath, $"General error: {ex.Message}");
        }
    }

    // Table creation with progress and logging
    static async Task CreateTables(List<(string schema, string tableName)> tables, string sqlConnectionString, string postgresConnectionString, string logFilePath, List<(string schema, string tableName)> errorQueue, int totalTables)
    {
        int currentTableIndex = 0;

        foreach (var table in tables)
        {
            currentTableIndex++;
            double progressPercentage = (double)currentTableIndex / totalTables * 100;
            Console.WriteLine($"Creating table {currentTableIndex}/{totalTables} ({progressPercentage:F2}%): {table.schema}.{table.tableName}");
            Log(logFilePath, $"Creating table {table.schema}.{table.tableName} ({progressPercentage:F2}%)");

            try
            {
                using (var sqlConnection = new SqlConnection(sqlConnectionString))
                using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
                {
                    await sqlConnection.OpenAsync();
                    await postgresConnection.OpenAsync();

                    // Check for schema creation
                    var createSchemaScript = $"CREATE SCHEMA IF NOT EXISTS \"{table.schema}\"";
                    using (var schemaCommand = new NpgsqlCommand(createSchemaScript, postgresConnection))
                    {
                        await schemaCommand.ExecuteNonQueryAsync();
                    }

                    // Table creation logic
                    var columns = sqlConnection.Query($@"
                        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{table.schema}' AND TABLE_NAME = '{table.tableName}'");

                    string createTableScript = $"CREATE TABLE IF NOT EXISTS \"{table.schema}\".\"{table.tableName}\" (";
                    foreach (var column in columns)
                    {
                        string columnName = column.COLUMN_NAME;
                        string dataType = ConvertSqlTypeToPostgres(column.DATA_TYPE, column.NUMERIC_PRECISION, column.NUMERIC_SCALE);
                        createTableScript += $"\"{columnName}\" {dataType},";
                    }
                    createTableScript = createTableScript.TrimEnd(',') + ");";

                    using (var createCommand = new NpgsqlCommand(createTableScript, postgresConnection))
                    {
                        await createCommand.ExecuteNonQueryAsync();
                    }
                    Log(logFilePath, $"Table {table.schema}.{table.tableName} created successfully.");
                }
            }
            catch (Exception ex)
            {
                errorQueue.Add(table);
                Log(logFilePath, $"Error creating table {table.schema}.{table.tableName}: {ex.Message}");
            }
        }
    }

    // Data transfer logic with batching
    static async Task TransferData(List<(string schema, string tableName)> tables, string sqlConnectionString, string postgresConnectionString, string logFilePath, List<(string schema, string tableName)> errorQueue, int totalTables)
    {
        int currentTableIndex = 0;

        foreach (var table in tables)
        {
            currentTableIndex++;
            double progressPercentage = (double)currentTableIndex / totalTables * 100;
            Console.WriteLine($"Transferring data for table {currentTableIndex}/{totalTables} ({progressPercentage:F2}%): {table.schema}.{table.tableName}");
            Log(logFilePath, $"Transferring data for table {table.schema}.{table.tableName} ({progressPercentage:F2}%)");

            try
            {
                using (var sqlConnection = new SqlConnection(sqlConnectionString))
                using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
                {
                    await sqlConnection.OpenAsync();
                    await postgresConnection.OpenAsync();

                    var rowCount = await sqlConnection.QuerySingleAsync<int>($"SELECT COUNT(*) FROM [{table.schema}].[{table.tableName}]");
                    if (rowCount == 0)
                    {
                        Log(logFilePath, $"Table {table.schema}.{table.tableName} has no data, skipping.");
                        continue;
                    }

                    var columnsForInsert = sqlConnection.Query($@"
                        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{table.schema}' AND TABLE_NAME = '{table.tableName}'");

                    int totalChunks = (int)Math.Ceiling((double)rowCount / 10000); // 1000 satır ile parçalama

                    for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
                    {
                        string query = $@"
                            SELECT * FROM [{table.schema}].[{table.tableName}]
                            ORDER BY 1
                            OFFSET {chunkIndex * 1000} ROWS FETCH NEXT 1000 ROWS ONLY";

                        using (var sqlCommand = new SqlCommand(query, sqlConnection))
                        {
                            sqlCommand.CommandTimeout = 3600;
                            using (var reader = await sqlCommand.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess))
                            {
                                var valueSets = new List<string>();
                                while (await reader.ReadAsync())
                                {
                                    var values = new List<string>();
                                    foreach (var column in columnsForInsert)
                                    {
                                        var value = reader[column.COLUMN_NAME];

                                        if (value == null || Convert.IsDBNull(value))
                                        {
                                            values.Add("NULL");
                                        }
                                        else if (value is string stringValue)
                                        {
                                            values.Add($"'{stringValue.Replace("'", "''")}'");
                                        }
                                        else if (value is DateTime)
                                        {
                                            values.Add($"'{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss")}'");
                                        }
                                        else if (value is decimal || value is double || value is float)
                                        {
                                            values.Add(value.ToString().Replace(",", "."));
                                        }
                                        else
                                        {
                                            values.Add($"'{value}'");
                                        }
                                    }
                                    valueSets.Add($"({string.Join(",", values)})");
                                }

                                if (valueSets.Any())
                                {
                                    await ExecuteBatchInsertAsync(postgresConnection, table.schema, table.tableName, valueSets, columnsForInsert);
                                }
                            }
                        }

                        Log(logFilePath, $"Chunk {chunkIndex + 1}/{totalChunks} for table {table.schema}.{table.tableName} transferred.");
                    }
                }
            }
            catch (Exception ex)
            {
                errorQueue.Add(table);
                Log(logFilePath, $"Error transferring data for table {table.schema}.{table.tableName}: {ex.Message}");
            }
        }
    }

    static async Task ExecuteBatchInsertAsync(NpgsqlConnection postgresConnection, string schema, string tableName, List<string> valueSets, IEnumerable<dynamic> columnsForInsert)
    {
        try
        {
            using (var transaction = await postgresConnection.BeginTransactionAsync())
            {
                string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" ({string.Join(",", columnsForInsert.Select(c => $"\"{c.COLUMN_NAME}\""))}) VALUES {string.Join(",", valueSets)};";
                using (var command = new NpgsqlCommand(insertScript, postgresConnection, transaction))
                {
                    command.CommandTimeout = 3600;
                    await command.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
            }
        }
        catch (Exception ex)
        {
            Log("migration_log.txt", $"Error during batch insert in table {tableName}: {ex.Message}");
        }
    }

    // Hatalı tablo oluşturma işlemlerini tekrar deneme
    static async Task RetryCreateTables(List<(string schema, string tableName)> errorTablesQueue, string sqlConnectionString, string postgresConnectionString, string logFilePath)
    {
        var retryQueue = new List<(string schema, string tableName)>(errorTablesQueue);
        foreach (var table in retryQueue)
        {
            int retryCount = 0;
            bool success = false;
            while (retryCount < maxRetryCount && !success)
            {
                try
                {
                    using (var sqlConnection = new SqlConnection(sqlConnectionString))
                    using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
                    {
                        await sqlConnection.OpenAsync();
                        await postgresConnection.OpenAsync();

                        string schema = table.schema;
                        string tableName = table.tableName;

                        var createSchemaScript = $"CREATE SCHEMA IF NOT EXISTS \"{schema}\"";
                        using (var schemaCommand = new NpgsqlCommand(createSchemaScript, postgresConnection))
                        {
                            await schemaCommand.ExecuteNonQueryAsync();
                        }

                        var columns = sqlConnection.Query($@"
                            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'");

                        string createTableScript = $"CREATE TABLE IF NOT EXISTS \"{schema}\".\"{tableName}\" (";
                        foreach (var column in columns)
                        {
                            string columnName = column.COLUMN_NAME;
                            string dataType = ConvertSqlTypeToPostgres(column.DATA_TYPE, column.NUMERIC_PRECISION, column.NUMERIC_SCALE);
                            createTableScript += $"\"{columnName}\" {dataType},";
                        }
                        createTableScript = createTableScript.TrimEnd(',') + ");";

                        using (var createCommand = new NpgsqlCommand(createTableScript, postgresConnection))
                        {
                            await createCommand.ExecuteNonQueryAsync();
                        }

                        Log(logFilePath, $"Retry succeeded for table {table.schema}.{table.tableName}");
                        success = true;
                    }
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Log(logFilePath, $"Retry {retryCount} failed for table {table.schema}.{table.tableName}: {ex.Message}");
                }
            }
        }
    }

    // Hatalı veri transferlerini tekrar deneme
    static async Task RetryTransferData(List<(string schema, string tableName)> errorTransferQueue, string sqlConnectionString, string postgresConnectionString, string logFilePath)
    {
        var retryQueue = new List<(string schema, string tableName)>(errorTransferQueue);
        foreach (var table in retryQueue)
        {
            int retryCount = 0;
            bool success = false;
            while (retryCount < maxRetryCount && !success)
            {
                try
                {
                    using (var sqlConnection = new SqlConnection(sqlConnectionString))
                    using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
                    {
                        await sqlConnection.OpenAsync();
                        await postgresConnection.OpenAsync();

                        var rowCount = await sqlConnection.QuerySingleAsync<int>($"SELECT COUNT(*) FROM [{table.schema}].[{table.tableName}]");
                        if (rowCount == 0)
                        {
                            Log(logFilePath, $"Table {table.schema}.{table.tableName} has no data, skipping.");
                            success = true;
                            continue;
                        }

                        var columnsForInsert = sqlConnection.Query($@"
                            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{table.schema}' AND TABLE_NAME = '{table.tableName}'");

                        using (var sqlCommand = new SqlCommand($"SELECT * FROM [{table.schema}].[{table.tableName}]", sqlConnection))
                        {
                            sqlCommand.CommandTimeout = 1800;
                            using (var reader = await sqlCommand.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess))
                            {
                                const int batchSize = 1000;
                                int rowCountBatch = 0;
                                var valueSets = new List<string>();

                                while (await reader.ReadAsync())
                                {
                                    var values = new List<string>();
                                    foreach (var column in columnsForInsert)
                                    {
                                        var value = reader[column.COLUMN_NAME];
                                        values.Add(value == null || Convert.IsDBNull(value) ? "NULL" : $"'{value}'");
                                    }

                                    valueSets.Add($"({string.Join(",", values)})");
                                    rowCountBatch++;

                                    if (rowCountBatch % batchSize == 0)
                                    {
                                        await ExecuteBatchInsertAsync(postgresConnection, table.schema, table.tableName, valueSets, columnsForInsert);
                                        valueSets.Clear();
                                    }
                                }

                                if (valueSets.Any())
                                {
                                    await ExecuteBatchInsertAsync(postgresConnection, table.schema, table.tableName, valueSets, columnsForInsert);
                                }
                            }
                        }

                        Log(logFilePath, $"Retry succeeded for data transfer {table.schema}.{table.tableName}");
                        success = true;
                    }
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Log(logFilePath, $"Retry {retryCount} failed for data transfer {table.schema}.{table.tableName}: {ex.Message}");
                }
            }
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
                return $"numeric({numericPrecision}, {numericScale})";
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
            case "image":
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
        lock (fileLock)
        {
            File.AppendAllText(filePath, $"{DateTime.Now}: {message}\n");
        }
        Console.WriteLine(message); // Log to console as well
    }

}
