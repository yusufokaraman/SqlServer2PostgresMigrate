using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Collections.Generic;
using System.IO;

internal class Program
{
    private static void Main(string[] args)
    {
        var logFilePath = "migration_log.txt";
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

                foreach (var table in tables)
                {
                    string schema = table.TABLE_SCHEMA;
                    string tableName = table.TABLE_NAME;

                    Console.WriteLine($"Processing table: {schema}.{tableName}");
                    Log(logFilePath, $"Processing table: {schema}.{tableName}");

                    // PostgreSQL'de şemayı oluştur
                    var createSchemaScript = $"CREATE SCHEMA IF NOT EXISTS \"{schema}\"";
                    using (var schemaCommand = new NpgsqlCommand(createSchemaScript, postgresConnection))
                    {
                        schemaCommand.ExecuteNonQuery();
                    }

                    // PostgreSQL'de tablo var mı kontrolü
                    string checkTableExistsScript = $@"
                        SELECT EXISTS (
                            SELECT 1 
                            FROM information_schema.tables 
                            WHERE table_schema = '{schema}' 
                            AND table_name = '{tableName}'
                        );";

                    using (var checkTableCommand = new NpgsqlCommand(checkTableExistsScript, postgresConnection))
                    {
                        bool tableExists = (bool)checkTableCommand.ExecuteScalar();

                        if (!tableExists)
                        {
                            // SQL Server'dan tabloyu al ve PostgreSQL'de oluştur
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
                                createCommand.ExecuteNonQuery();
                            }
                        }
                    }

                    // Veritabanında veri olup olmadığını kontrol et
                    var rowCount = sqlConnection.QuerySingle<int>($"SELECT COUNT(*) FROM [{schema}].[{tableName}]");
                    if (rowCount == 0)
                    {
                        Console.WriteLine($"Table {schema}.{tableName} has no data, skipping insert.");
                        Log(logFilePath, $"Table {schema}.{tableName} has no data, skipping insert.");
                        continue; 
                    }

                    // SQL Server'dan büyük veriyi memory'e yüklemeden okuma
                    var columnsForInsert = sqlConnection.Query($@"
                        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'");

                    using (var transaction = postgresConnection.BeginTransaction())
                    using (var sqlCommand = new SqlCommand($"SELECT * FROM [{schema}].[{tableName}]", sqlConnection))
                    {
                        sqlCommand.CommandTimeout = 1800;
                        using (var reader = sqlCommand.ExecuteReader(System.Data.CommandBehavior.SequentialAccess))
                        {
                            const int batchSize = 1000;
                            int rowCountBatch = 0;
                            var valueSets = new List<string>();

                            while (reader.Read())
                            {
                                var values = new List<string>();

                                foreach (var column in columnsForInsert)
                                {
                                    var value = reader[column.COLUMN_NAME];

                                    // Null ve DBNull kontrolleri
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
                                    string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" ({string.Join(",", columnsForInsert.Select(c => $"\"{c.COLUMN_NAME}\""))}) VALUES {string.Join(",", valueSets)};";
                                    using (var command = new NpgsqlCommand(insertScript, postgresConnection, transaction))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    valueSets.Clear();
                                }
                            }

                            if (valueSets.Any())
                            {
                                string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" ({string.Join(",", columnsForInsert.Select(c => $"\"{c.COLUMN_NAME}\""))}) VALUES {string.Join(",", valueSets)};";
                                using (var command = new NpgsqlCommand(insertScript, postgresConnection, transaction))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }

                            transaction.Commit();
                        }
                    }
                    Console.WriteLine($"Table {schema}.{tableName} processed successfully.");
                    Log(logFilePath, $"Table {schema}.{tableName} processed successfully.");
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
        // SQL tiplerini PostgreSQL tiplerine dönüştürme
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
        File.AppendAllText(filePath, $"{DateTime.Now}: {message}\n");
    }
}
