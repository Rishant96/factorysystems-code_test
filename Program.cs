using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using System.IO;
using CsvHelper;

namespace Code
{

    class Program
    {

        static void Main(string[] args)
        {
            Console.Write("Enter the database location (leave empty for '.\\SurfaceRoughnessDB.db3'): ");
            string dbPath = Console.ReadLine();

            Console.Write("Enter the size of filter (default=3): ");
            Decimal filterSize;
            decimal.TryParse(Console.ReadLine(), out filterSize);
            if (filterSize <= 0.0M) {
                filterSize = 3.0M;
            }
            

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
                SqliteDataReader sqliteReader = getMeasurementsCmd.ExecuteReader();
                
                var dataTable = new System.Data.DataTable();
                dataTable.Load(sqliteReader);
                sqliteReader.Close();

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

                var reader = dataTable.CreateDataReader();
                while (reader.Read())
                {
                    int currTestUID = (int) reader.GetInt64(0);
                    bool isNewTestUID = testUIDs.Add(currTestUID);

                    decimal height = (decimal) reader.GetDouble(3);
                    decimal xPos = (decimal) reader.GetDouble(1);
                    decimal yPos = (decimal) reader.GetDouble(2);

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

                reader.Close();
                reader = dataTable.CreateDataReader();
                var errors = new Dictionary<int, (decimal absErr, double rmsErr, int outliers)>();

                while (reader.Read()) {
                    int currID = (int) reader.GetInt64(0);
                    if (testUIDs.Contains(currID)) {
                        if (! errors.ContainsKey(currID)) {
                            errors.Add(currID, (0.0M, 0.0, 0));
                        }
                        decimal height = (decimal) reader.GetDouble(3);
                        // err: Calculates the difference between the each height observation and the mean
                        decimal err = height - 
                            (testInfo[currID].heightSum / testInfo[currID].count);
                        errors[currID] = (
                            // Sum of all absolute errors
                            errors[currID].absErr + Math.Abs(err), 
                            // Sum of all root mean squared errors
                            errors[currID].rmsErr + Math.Pow((double) err, 2),
                            0
                        );
                    }
                    else {
                        throw new Exception("Something went wrong");
                    }
                }
                
                reader.Close();
                reader = dataTable.CreateDataReader();
                while (reader.Read()) {
                    int currID = (int) reader.GetInt64(0);
                    var height = (decimal) reader.GetDouble(3);
                    
                    if (errors.ContainsKey(currID)) {
                        var avg_height = (testInfo[currID].heightSum / testInfo[currID].count);
                        var rms_roughness = (decimal) Math.Pow(
                            (errors[currID].rmsErr / testInfo[currID].count), 0.5);

                        decimal err = height - 
                            (testInfo[currID].heightSum / testInfo[currID].count);
                    
                        if (Math.Abs(avg_height - height) > rms_roughness * filterSize) {
                            errors[currID] = (
                                errors[currID].absErr - Math.Abs(err),
                                errors[currID].rmsErr - Math.Pow((double) err, 2),
                                errors[currID].outliers + 1
                            );
                        }
                    }
                    else {
                        throw new Exception("Something went wrong");
                    }
                }

                reader.Close();
                reader = dataTable.CreateDataReader();

                var splitStr = connStr.Split('\\');
                var index = splitStr.Length - 1;
                var csvPath = "";
                for (int i=0; i < index; i++) {
                    csvPath += splitStr[i] + '\\';
                }
                csvPath += "roughness_report.csv";

                using (StreamWriter sw = new StreamWriter(csvPath, false, new UTF8Encoding(true))) {
                    using (var csvWriter = new CsvHelper.CsvWriter(sw, 
                        new CsvHelper.Configuration.CsvConfiguration(
                            new System.Globalization.CultureInfo("en-US")) 
                                { HasHeaderRecord = true })) {

                        csvWriter.WriteField("test_uid");
                        csvWriter.WriteField("sTime");
                        csvWriter.WriteField("PlaneID");
                        csvWriter.WriteField("Operator");
                        csvWriter.WriteField("min_height");
                        csvWriter.WriteField("min_height_X");
                        csvWriter.WriteField("min_height_Y");
                        csvWriter.WriteField("max_height");
                        csvWriter.WriteField("max_height_X");
                        csvWriter.WriteField("max_height_Y");
                        csvWriter.WriteField("mean_height");
                        csvWriter.WriteField("range_height");
                        csvWriter.WriteField("outlier_count");
                        csvWriter.WriteField("total_count");
                        csvWriter.WriteField("avg_roughness");
                        csvWriter.WriteField("rms_roughness");
                        csvWriter.WriteField("is_test_valid");
                        csvWriter.NextRecord();

                        
                        string tests = "SELECT test_uid, sTime, PlaneID, Operator FROM Tests"; 
                        using var getTestsCmd = new SqliteCommand(tests, connection);
                        sqliteReader = getTestsCmd.ExecuteReader();
                        
                        while (sqliteReader.Read()) {
                            int test_uid = (int) sqliteReader.GetInt64(0);                                
                            DateTime test_sTime = sqliteReader.GetDateTime(1);
                            string test_planeId = sqliteReader.GetString(2);
                            string test_operator = "<UNKNOWN>";

                            try {
                                test_operator = sqliteReader.GetString(3);
                            }
                            catch (InvalidOperationException) {}

                            try {
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

                                var outlier_count = errors[test_uid].outliers;
                                var is_test_valid = testInfo[test_uid].count >= 1000;

                                csvWriter.WriteField($"{test_uid}");
                                csvWriter.WriteField($"{test_sTime}");
                                csvWriter.WriteField($"{test_planeId}");
                                csvWriter.WriteField($"{test_operator}");
                                csvWriter.WriteField($"{min_height}");
                                csvWriter.WriteField($"{min_height_X}");
                                csvWriter.WriteField($"{min_height_Y}");
                                csvWriter.WriteField($"{max_height}");
                                csvWriter.WriteField($"{max_height_X}");
                                csvWriter.WriteField($"{max_height_Y}");
                                csvWriter.WriteField($"{mean_height}");
                                csvWriter.WriteField($"{range_height}");
                                csvWriter.WriteField($"{outlier_count}");
                                csvWriter.WriteField($"{testInfo[test_uid].count}");
                                csvWriter.WriteField($"{avg_roughness}");
                                csvWriter.WriteField($"{rms_roughness}");
                                csvWriter.WriteField($"{is_test_valid}");
                                csvWriter.NextRecord();
                            }
                            catch (KeyNotFoundException) {
                                csvWriter.WriteField($"{test_uid}");
                                csvWriter.WriteField($"{test_sTime}");
                                csvWriter.WriteField($"{test_planeId}");
                                csvWriter.WriteField($"{test_operator}");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("<N/A>");
                                csvWriter.WriteField("False");
                                csvWriter.NextRecord();
                            }
                        }
                        sw.Flush();
                        sqliteReader.Close();

                        Console.WriteLine($"Filtered report generated at {csvPath}");
                    }
                }
                reader.Close();
            }

            Console.ReadLine();
        }
    }
}
