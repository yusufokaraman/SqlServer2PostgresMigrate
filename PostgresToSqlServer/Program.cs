using Npgsql;
using PostgresToSqlServer;
using System.Data.SqlClient;

internal class Program
{
    static async Task Main(string[] args)
    {
        //var sqlConnectionString = "Server=10.35.36.3,1435;Database=MSO;User Id=sa;Password=Abc123def!!!;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        //var pgConnectionString = "Host=localhost;Port=5434;Database=MSO;Username=sa;Password=Abc123def!!!;Timeout=500";

        var sqlConnectionString = "Server=127.0.0.1;Database=Dvdrental;User Id=deneme;Password=123456;Integrated Security=False;TrustServerCertificate=True;Connect Timeout=120;";
        var pgConnectionString = "Host=localhost;Port=5432;Database=dvdrental;Username=postgres;Password=postgre123;Timeout=500";

        using (var pgConn = new NpgsqlConnection(pgConnectionString))
        using (var sqlConn = new SqlConnection(sqlConnectionString))
        {
            await pgConn.OpenAsync();
            await sqlConn.OpenAsync();

            var migrator = new DatabaseMigrator();
            await migrator.MigrateDatabaseAsync(pgConn, sqlConn);
        }

        Console.WriteLine("Migration completed!");
        Console.ReadLine();
        Console.ReadKey();
    }
}