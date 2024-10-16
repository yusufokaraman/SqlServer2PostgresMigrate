using DbMigrator;
using Npgsql;
using System.Data.SqlClient;

internal class Program
{
    static async Task Main(string[] args)
    {
        var sqlConnectionString = "Server=10.35.36.3,1435;Database=MSO;User Id=sa;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        var pgConnectionString = "Host=localhost;Port=5434;Database=MSO;Username=sa;Password=Abc123def!!!;Timeout=500";

        using (var sqlConn = new SqlConnection(sqlConnectionString))
        using (var pgConn = new NpgsqlConnection(pgConnectionString))
        {
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            var migrator = new DatabaseMigrator();
            await migrator.MigrateDatabaseAsync(sqlConn, pgConn);
        }

        Console.WriteLine("Migration completed!");
    }
}