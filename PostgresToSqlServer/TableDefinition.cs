namespace PostgresToSqlServer
{
    public class TableDefinition
    {
        public string Schema { get; set; }
        public string Name { get; set; }
        public List<ColumnDefinition> Columns { get; set; } = new List<ColumnDefinition>();
    }


    public class ColumnDefinition
    {
        public string Name { get; set; }
        public string PgDataType { get; set; }
        public bool IsNullable { get; set; }
        public bool IsPrimaryKey { get; set; }
        public int? PrimaryKeyOrdinal { get; set; }
    }

    public class ForeignKeyDefinition
    {
        public string ConstraintName { get; set; }
        public string ForeignKeySchema { get; set; }
        public string ForeignKeyTable { get; set; }
        public string ForeignKeyColumn { get; set; }
        public string PrimaryKeySchema { get; set; }
        public string PrimaryKeyTable { get; set; }
        public string PrimaryKeyColumn { get; set; }
    }


}
