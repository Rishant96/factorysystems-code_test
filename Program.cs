using System;
using Microsoft.Data.Sqlite;
using static System.Console;

namespace Code
{
    class Program
    {
        static void Main(string[] args)
        {
            var connStr =  @".\SurfaceRoughnessDB.db3";
            var connectionStringBuilder = new SqliteConnectionStringBuilder() {
                DataSource=connStr,
                Mode=SqliteOpenMode.ReadOnly
            };
            using (var connection = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                connection.Open();

                string measurements = "SELECT * FROM Measurements LIMIT 10";

                using var cmd = new SqliteCommand(measurements, connection);
                SqliteDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    WriteLine($"{reader.GetInt32(0)} {reader.GetInt32(1)} {reader.GetFloat(2)} {reader.GetFloat(3)} {reader.GetFloat(4)}");
                }
            }
        }
    }
}
