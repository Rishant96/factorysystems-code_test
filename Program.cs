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

                using var getMeasurementsCmd = new SqliteCommand(measurements, connection);
                SqliteDataReader reader = getMeasurementsCmd.ExecuteReader();

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
                        int count,
                        decimal avgRoughness,
                        decimal rmsRoughness
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
                            1,
                            -1.0M,
                            -1.0M
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
                           testInfo[currTestUID].count + 1,
                           -1.0M,
                           -1.0M
                       );
                    }
                }

                string measurementsHeightsOnly = "SELECT test_uid, height FROM Measurements LIMIT 1005"; 

                using var getHeightsOnlyCmd = new SqliteCommand(measurementsHeightsOnly, connection);
                reader = getHeightsOnlyCmd.ExecuteReader();

                var errors = new Dictionary<int, (decimal absErr, double rmsErr)>();

                while (reader.Read()) {
                    int currID = reader.GetInt32(0);
                    if (testUIDs.Contains(currID)) {
                        if (! errors.ContainsKey(currID)) {
                            errors.Add(currID, (0.0M, 0.0));
                        }
                        decimal height = reader.GetDecimal(1);
                        decimal err = height - 
                            (testInfo[currID].heightSum / testInfo[currID].count);
                        errors[currID] = (
                            errors[currID].absErr + Math.Abs(err), 
                            errors[currID].rmsErr + Math.Pow((double) err, 2)
                        );
                    }
                    else {
                        throw new Exception("Something went wrong");
                    }
                }
            }
        }
    }
}
