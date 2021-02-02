using System;
using System.Collections.Generic;
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
                string measurements = "SELECT test_uid, x, y, height FROM Measurements LIMIT 1005"; 

                using var cmd = new SqliteCommand(measurements, connection);
                SqliteDataReader reader = cmd.ExecuteReader();

                var testUIDs = new SortedSet<int>();

                // testInfo,
                // Key: testUID, 
                // Value: (minHeight, (minHeightX, minHeightY) minPos, maxHeight, (maxHeightX, maxHeightY) maxPos, heightSum, count)
                var testInfo = new Dictionary<int , 
                    (
                        decimal minHeight, 
                        (decimal minHeightX, decimal minHeightY) minPos, 
                        decimal maxHeight, 
                        (decimal maxHeightX, decimal maxHeightY) maxPos, 
                        decimal heightSum,
                        int count
                    )
                > ();

                while (reader.Read())
                {
                    int currTestUID = reader.GetInt32(0);
                    bool isNewTestUID = testUIDs.Add(currTestUID);


                    decimal height = reader.GetDecimal(3);
                    decimal xPos = reader.GetDecimal(1);
                    decimal yPos = reader.GetDecimal(2);

                    if (isNewTestUID) {
                        testInfo[currTestUID] = (
                            height,
                            (xPos, yPos),
                            height,
                            (xPos, yPos),
                            height,
                            1
                        );
                    }
                    else {
                        bool isMin = testInfo[currTestUID].minHeight > height;
                        bool isMax = testInfo[currTestUID].maxHeight < height;
                       
                       testInfo[currTestUID] = (
                           isMin ? height : testInfo[currTestUID].minHeight,
                           !isMin ? testInfo[currTestUID].minPos : (xPos, yPos),
                           isMax ? height : testInfo[currTestUID].maxHeight,
                           !isMax ? testInfo[currTestUID].maxPos : (xPos, yPos),
                           testInfo[currTestUID].heightSum + height,
                           testInfo[currTestUID].count + 1 
                       );
                    }
                }
                WriteLine("" + 
                    testInfo[1].minHeight + ", " +
                    testInfo[1].minPos + ", " +
                    testInfo[1].maxHeight + ", " +
                    testInfo[1].maxPos + ", " +
                    testInfo[1].heightSum + ", " +
                    testInfo[1].count + ", " +
                    testInfo[1].heightSum / testInfo[1].count 
                );

                WriteLine(testInfo[1].maxHeight - testInfo[1].minHeight);
            }
        }
    }
}
