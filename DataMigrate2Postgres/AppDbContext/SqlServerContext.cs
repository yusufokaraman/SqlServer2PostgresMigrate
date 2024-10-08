﻿using Microsoft.EntityFrameworkCore;

namespace DataMigrate2Postgres.AppDbContext
{
    public class SqlServerContext : DbContext
    {
        public SqlServerContext(DbContextOptions<SqlServerContext> options) : base(options) { }
    }
}
