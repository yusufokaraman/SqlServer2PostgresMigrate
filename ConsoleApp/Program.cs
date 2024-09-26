using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

internal class Program
{
    private static readonly object fileLock = new object(); 
    private static SemaphoreSlim semaphore = new SemaphoreSlim(20); 

    private static async Task Main(string[] args)
    {
        var logFilePath = "migration_log.txt";
        File.WriteAllText(logFilePath, "Migration process started...\n");

        var sqlConnectionString = "Server=10.254.183.242;Database=Emptor_ProbilServis_Prod;User Id=ahmet.bilik;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        var postgresConnectionString = "Host=10.254.183.242;Port=5433;Database=EmptorProbilServisProd;Username=sa;Password=Abc123def!!!;Timeout=500";

        List<(string schema, string tableName)> errorTablesQueue = new List<(string schema, string tableName)>();

        try
        {
            using (var sqlConnection = new SqlConnection(sqlConnectionString))
            using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
            {
                await sqlConnection.OpenAsync();
                await postgresConnection.OpenAsync();
                Log(logFilePath, "Connected to both SQL Server and PostgreSQL");

                var tables = sqlConnection.Query("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'").ToList();
                int totalTables = tables.Count;
                Console.WriteLine($"Total number of tables to process: {totalTables}");
                Log(logFilePath, $"Total number of tables to process: {totalTables}");

                var tasks = new List<Task>();
                int currentTableIndex = 0;

                foreach (var table in tables)
                {
                    currentTableIndex++;
                    string schema = table.TABLE_SCHEMA;
                    string tableName = table.TABLE_NAME;

                    double progressPercentage = (double)currentTableIndex / totalTables * 100;
                    Console.WriteLine($"Processing table {currentTableIndex}/{totalTables} ({progressPercentage:F2}% complete): {schema}.{tableName}");
                    Log(logFilePath, $"Processing table {currentTableIndex}/{totalTables} ({progressPercentage:F2}% complete): {schema}.{tableName}");

                    await semaphore.WaitAsync();

                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            using (var sqlConn = new SqlConnection(sqlConnectionString))
                            using (var pgConn = new NpgsqlConnection(postgresConnectionString))
                            {
                                await sqlConn.OpenAsync();
                                await pgConn.OpenAsync();

                                EnsureOpenConnection(sqlConn, pgConn);
                                await ProcessTableAsync(sqlConn, pgConn, schema, tableName, logFilePath);
                            }
                        }
                        catch (Exception ex)
                        {
                            errorTablesQueue.Add((schema, tableName));
                            Log(logFilePath, $"Error processing table {schema}.{tableName}: {ex.Message}");
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                // Hata alınan tabloları tekrar dene
                foreach (var (schema, tableName) in errorTablesQueue)
                {
                    try
                    {
                        Console.WriteLine($"Retrying table {schema}.{tableName}");
                        EnsureOpenConnection(sqlConnection, postgresConnection);
                        await ProcessTableAsync(sqlConnection, postgresConnection, schema, tableName, logFilePath);
                    }
                    catch (Exception retryEx)
                    {
                        Log(logFilePath, $"Retry failed for table {schema}.{tableName}: {retryEx.Message}");
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

    static void EnsureOpenConnection(SqlConnection sqlConnection, NpgsqlConnection postgresConnection)
    {
        if (sqlConnection.State != System.Data.ConnectionState.Open)
        {
            sqlConnection.Open();
        }

        if (postgresConnection.State != System.Data.ConnectionState.Open)
        {
            postgresConnection.Open();
        }
    }

    static async Task ProcessTableAsync(SqlConnection sqlConnection, NpgsqlConnection postgresConnection, string schema, string tableName, string logFilePath)
    {
        var createSchemaScript = $"CREATE SCHEMA IF NOT EXISTS \"{schema}\"";
        using (var schemaCommand = new NpgsqlCommand(createSchemaScript, postgresConnection))
        {
            await schemaCommand.ExecuteNonQueryAsync();
        }

        string checkTableExistsScript = $@"
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                AND table_name = '{tableName}'
            );";

        using (var checkTableCommand = new NpgsqlCommand(checkTableExistsScript, postgresConnection))
        {
            bool tableExists = (bool)await checkTableCommand.ExecuteScalarAsync();

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

                using (var createCommand = new NpgsqlCommand(createTableScript, postgresConnection))
                {
                    await createCommand.ExecuteNonQueryAsync();
                }
            }
        }

        var rowCount = await sqlConnection.QuerySingleAsync<int>($"SELECT COUNT(*) FROM [{schema}].[{tableName}]");
        if (rowCount == 0)
        {
            Console.WriteLine($"Table {schema}.{tableName} has no data, skipping insert.");
            Log(logFilePath, $"Table {schema}.{tableName} has no data, skipping insert.");
            return;
        }

        var columnsForInsert = sqlConnection.Query($@"
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'");

        using (var sqlCommand = new SqlCommand($"SELECT * FROM [{schema}].[{tableName}]", sqlConnection))
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

                        if (value == null || Convert.IsDBNull(value))
                        {
                            values.Add("NULL");
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
                            values.Add($"'{value.ToString().Replace("'", "''")}'");
                        }
                    }

                    valueSets.Add($"({string.Join(",", values)})");
                    rowCountBatch++;

                    if (rowCountBatch % batchSize == 0)
                    {
                        await ExecuteBatchInsertAsync(postgresConnection, schema, tableName, valueSets, columnsForInsert);
                        valueSets.Clear();
                    }
                }

                if (valueSets.Any())
                {
                    await ExecuteBatchInsertAsync(postgresConnection, schema, tableName, valueSets, columnsForInsert);
                }
            }
        }

        Console.WriteLine($"Table {schema}.{tableName} processed successfully.");
        Log(logFilePath, $"Table {schema}.{tableName} processed successfully.");
    }

    static async Task ExecuteBatchInsertAsync(NpgsqlConnection postgresConnection, string schema, string tableName, List<string> valueSets, IEnumerable<dynamic> columnsForInsert)
    {
        using (var transaction = await postgresConnection.BeginTransactionAsync())
        {
            string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" ({string.Join(",", columnsForInsert.Select(c => $"\"{c.COLUMN_NAME}\""))}) VALUES {string.Join(",", valueSets)};";
            using (var command = new NpgsqlCommand(insertScript, postgresConnection, transaction))
            {
                await command.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
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
                return "timestamp";
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
        lock (fileLock) // Dosya erişimini kilitliyoruz
        {
            File.AppendAllText(filePath, $"{DateTime.Now}: {message}\n");
        }
    }
}
