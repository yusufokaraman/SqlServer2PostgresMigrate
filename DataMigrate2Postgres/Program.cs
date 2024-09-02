using DataMigrate2Postgres;
using DataMigrate2Postgres.AppDbContext;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

internal class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            // appsettings.json dosyasının doğru okunuyor mu?
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            Console.WriteLine("SqlServer Connection String: " + config.GetConnectionString("SqlServer"));
            Console.WriteLine("Postgres Connection String: " + config.GetConnectionString("Postgres"));

            var serviceProvider = new ServiceCollection()
                .AddDbContext<SqlServerContext>(options => options.UseSqlServer(config.GetConnectionString("SqlServer")))
                .AddDbContext<PostgresContext>(options => options.UseNpgsql(config.GetConnectionString("Postgres")))
                .AddTransient<MigrationService>()
                .BuildServiceProvider();

            var sqlContext = serviceProvider.GetService<SqlServerContext>();
            var postgresContext = serviceProvider.GetService<PostgresContext>();

            // Bağlantı testleri
            Console.WriteLine("MSSQL veri tabanına bağlanılıyor...");
            await sqlContext.Database.OpenConnectionAsync();
            Console.WriteLine("MSSQL veri tabanı bağlantısı başarılı.");
            await sqlContext.Database.CloseConnectionAsync();

            Console.WriteLine("PostgreSQL veri tabanına bağlanılıyor...");
            await postgresContext.Database.OpenConnectionAsync();
            Console.WriteLine("PostgreSQL veri tabanı bağlantısı başarılı.");
            await postgresContext.Database.CloseConnectionAsync();

            var migrationService = serviceProvider.GetService<MigrationService>();
            Console.WriteLine("Veri taşıma işlemi başlatılıyor...");
            await migrationService.MigrateData();
            Console.WriteLine("Veri taşıma işlemi tamamlandı.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Bir hata oluştu: {ex.Message}");
        }
    }

    static string GetConnectionString(string name)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("AppSettings.json")
            .Build();

        return config.GetConnectionString(name);
    }
}
