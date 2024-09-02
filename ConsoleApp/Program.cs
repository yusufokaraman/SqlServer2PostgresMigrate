using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Globalization;
using System.Collections.Generic;

internal class Program
{
    private static void Main(string[] args)
    {
        var sqlConnectionString = "Server=10.35.36.6;Database=MSO;User Id=sa;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;";
        var postgresConnectionString = "Host=10.35.36.6;Port=5434;Database=MSO;Username=sa;Password=Abc123def!!!;";

        using (var sqlConnection = new SqlConnection(sqlConnectionString))
        using (var postgresConnection = new NpgsqlConnection(postgresConnectionString))
        {
            sqlConnection.Open();
            postgresConnection.Open();

            var tables = sqlConnection.Query("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");

            foreach (var table in tables)
            {
                string schema = table.TABLE_SCHEMA;
                string tableName = table.TABLE_NAME;

                Console.WriteLine($"Table: {schema}.{tableName}");

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

                        Console.WriteLine($"Create Table Script: {createTableScript}");

                        using (var command = new NpgsqlCommand(createTableScript, postgresConnection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }

                    var columnsForInsert = sqlConnection.Query($@"
                        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tableName}'");

                    var data = sqlConnection.Query($"SELECT * FROM \"{schema}\".\"{tableName}\"");

                    foreach (var row in data)
                    {
                        string insertScript = $"INSERT INTO \"{schema}\".\"{tableName}\" (";

                        foreach (var column in columnsForInsert)
                        {
                            insertScript += $"\"{column.COLUMN_NAME}\",";
                        }

                        insertScript = insertScript.TrimEnd(',') + ") VALUES (";

                        using (var command = new NpgsqlCommand())
                        {
                            command.Connection = postgresConnection;

                            var parameterNames = new List<string>();

                            foreach (var column in columnsForInsert)
                            {
                                string paramName = column.COLUMN_NAME;
                                parameterNames.Add($"@{paramName}");

                                var value = ((IDictionary<string, object>)row)[column.COLUMN_NAME];

                                if (Convert.IsDBNull(value) || string.IsNullOrWhiteSpace(value?.ToString()))
                                {
                                    command.Parameters.AddWithValue(paramName, DBNull.Value);
                                }
                                else if (value is DateTime dateTimeValue)
                                {
                                    command.Parameters.AddWithValue(paramName, dateTimeValue);
                                }
                                else if (value is decimal || value is double || value is float)
                                {
                                    command.Parameters.AddWithValue(paramName, value);
                                }
                                else
                                {
                                    command.Parameters.AddWithValue(paramName, value);
                                }
                            }

                            insertScript += string.Join(",", parameterNames) + ");";
                            command.CommandText = insertScript;
                            command.ExecuteNonQuery();
                        }
                    }
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
                return "char";  // veya 'character'
            case "uniqueidentifier":
                return "uuid";
            case "datetime":
            case "date":
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
                return "bytea";
            default:
                throw new NotSupportedException($"Unsupported SQL type: {sqlType}");
        }
    }

}
