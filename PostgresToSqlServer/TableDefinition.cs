using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresToSqlServer
{
        public class TableDefinition
        {
            public string Name { get; set; }
            public string Schema { get; set; }
            public List<ColumnDefinition> Columns { get; set; } = new List<ColumnDefinition>();
        }

        public class ColumnDefinition
        {
            public string Name { get; set; }
            public string SqlDataType { get; set; }
            public bool IsNullable { get; set; }
        }
}
