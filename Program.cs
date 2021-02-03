 using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using System.IO;
using static System.Console;

namespace Code
{

    class Program
    {

        static void Main(string[] args)
        {
            Write("Enter the database location (press Enter, leave empty for '.\\SurfaceRoughnessDB.db3'): ");
            string dbPath = ReadLine();
            

            var connStr =  dbPath == "" ? @".\SurfaceRoughnessDB.db3" : $@"{dbPath}";
            var connectionStringBuilder = new SqliteConnectionStringBuilder() {
                DataSource=connStr,
                Mode=SqliteOpenMode.ReadOnly
            };
            using (var connection = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {
                connection.Open();
                string measurements = "SELECT test_uid, x, y, height FROM Measurements"; 

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

                string measurementsHeightsOnly = "SELECT test_uid, height FROM Measurements"; 

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

                string getTests = "SELECT * FROM Tests"; 

                using var getTestCmd = new SqliteCommand(getTests, connection);
                reader = getTestCmd.ExecuteReader();

                var csv = new StringBuilder();
                csv.AppendLine($"test_uid, sTime, PlaneID, Operator, min_height, min_height_X, min_height_Y, max_height, max_height_X, max_height_Y, mean_height, range_height, avg_roughness, rms_roughness");

                while (reader.Read()) {
                    try {
                    int test_uid = reader.GetInt32(0);
                        if (testInfo[test_uid].count < 1000) continue;
                        
                        DateTime test_sTime = reader.GetDateTime(1);
                        string test_planeId = reader.GetString(2);
                        string test_operator = "<UNKNOWN>";
                        try {
                        test_operator = reader.GetString(3);
                        }
                        catch (InvalidOperationException) {}

                        var min_height = testInfo[test_uid].minHeight;
                        var min_height_X = testInfo[test_uid].minPos.minHeightX;
                        var min_height_Y = testInfo[test_uid].minPos.minHeightY;
                        
                        var max_height = testInfo[test_uid].maxHeight;
                        var max_height_X = testInfo[test_uid].maxPos.maxHeightX;
                        var max_height_Y = testInfo[test_uid].maxPos.maxHeightY;

                        var mean_height = testInfo[test_uid].heightSum / testInfo[test_uid].count;
                        var range_height = testInfo[test_uid].maxHeight - testInfo[test_uid].minHeight;

                        var avg_roughness = errors[test_uid].absErr / testInfo[test_uid].count;
                        var rms_roughness = Math.Pow(
                            (errors[test_uid].rmsErr / testInfo[test_uid].count), 0.5);

                        var newLine = $"{test_uid},{test_sTime},{test_planeId},{test_operator},{min_height},{min_height_X},{min_height_Y},{max_height},{max_height_X},{max_height_Y},{mean_height},{range_height},{avg_roughness},{rms_roughness}";
                        csv.AppendLine(newLine);
                    }
                    catch (KeyNotFoundException) {}
                }

                File.WriteAllText(".\\roughness_report.csv", csv.ToString());
                WriteLine($"Report generated at {connStr}");
            }
        }
    }
}
