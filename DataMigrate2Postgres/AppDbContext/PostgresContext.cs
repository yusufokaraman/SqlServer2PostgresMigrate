using Microsoft.EntityFrameworkCore;

namespace DataMigrate2Postgres.AppDbContext
{
    public class PostgresContext : DbContext
    {
        public PostgresContext(DbContextOptions<PostgresContext> options) : base(options) { }
    }
}
