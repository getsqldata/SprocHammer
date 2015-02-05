using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace SprocHammer
{
    public static class SqlHelper
    {
        public static void ExecuteNonQuery(this SqlConnection connection, string procName, int timeoutSeconds = 30, SqlTransaction transaction = null)
        {
            var cmd = new SqlCommand(procName, connection)
            {
                CommandTimeout = timeoutSeconds,
                CommandType = CommandType.StoredProcedure
            };
            if (transaction != null)
            {
                cmd.Transaction = transaction;
            }
            cmd.ExecuteNonQuery();
        }

        public static DataTable ExecuteTable(this SqlConnection connection, string procName, int timeoutSeconds = 30, SqlTransaction transaction = null)
        {
            var cmd = new SqlCommand(procName, connection)
            {
                CommandTimeout = timeoutSeconds,
                CommandType = CommandType.StoredProcedure,
            };
            if (transaction != null)
            {
                cmd.Transaction = transaction;
            }
            var adapter = new SqlDataAdapter {SelectCommand = cmd};
            var table = new DataTable();
            adapter.Fill(table);
            return table;
        }

        public static void Dump(this DataTable table, TextWriter writer)
        {
            writer.WriteLine(string.Join("\t", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));
            foreach (DataRow row in table.Rows)
            {
                writer.WriteLine(string.Join("\t", row.ItemArray.Select(i => i.ToString())));
            }
        }
    }
}