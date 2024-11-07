using DbMigrator;
using Microsoft.Data.SqlClient;
using Npgsql;

internal class Program
{
    static async Task Main(string[] args)
    {
        //var sqlConnectionString = "Server=10.35.36.3,1435;Database=MSO;User Id=sa;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        //var pgConnectionString = "Host=localhost;Port=5434;Database=MSO;Username=sa;Password=Abc123def!!!;Timeout=500";


        //var sqlConnectionString = "Server=10.35.36.3,1435;Database=MSO;User Id=sa;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        //var pgConnectionString = "Host=localhost;Port=5434;Database=MSO;Username=sa;Password=Abc123def!!!;Timeout=500";

        var sqlConnectionString = "Server=10.35.36.6;Database=MSO;User Id=sa;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;";
        var pgConnectionString = "Host=10.35.36.6;Port=5434;Database=MSO;Username=sa;Password=Abc123def!!!;Timeout=500";

        //var sqlConnectionString = "Server=127.0.0.1;Database=AdventureWorks2022;User Id=deneme;Password=123456;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        //var pgConnectionString = "Host=localhost;Port=5432;Database=AdventureWorks2022;Username=postgres;Password=postgre123;Timeout=500;";

        using (var sqlConn = new SqlConnection(sqlConnectionString))
        using (var pgConn = new NpgsqlConnection(pgConnectionString))
        {
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            var migrator = new DatabaseMigrator();
            await migrator.MigrateDatabaseAsync(sqlConn, pgConn);
        }

        Console.WriteLine("Migration completed!");
        Console.ReadKey();
    }
}