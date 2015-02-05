using System;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;

namespace SprocHammer
{
    public class Program
    {
        private static int Main(string[] args)
        {
            Console.WriteLine();

            string settingsPath;
            if (args.Length != 1 || !File.Exists(settingsPath = args[0]))
            {
                Console.WriteLine("usage: sprocHammer <settingsFilePath>");
                Console.WriteLine();
                Console.WriteLine("Settings format:");
                Console.WriteLine(new TestRunSettings());
                Console.WriteLine();
                return 1;
            }

            // load settings
            var settingsJson = File.ReadAllText(settingsPath);
            var settings = TestRunSettings.DeserializeJson(settingsJson);
            if (!settings.Validate(Console.Out))
            {
                return 1;
            }

            // run name
            if (string.IsNullOrEmpty(settings.RunName))
            {
                settings.RunName = Path.GetFileNameWithoutExtension(settingsPath);
            }

            // output file
            if (string.IsNullOrEmpty(settings.OutputPath))
            {
                settings.OutputPath = string.Format("{0}_{1}.log",
                    DateTime.Now.ToString("s").Replace(":", "-"), settings.RunName);
            }
            Console.WriteLine("Writing output to {0}", Path.GetFullPath(settings.OutputPath));

            // write settings
            using (var output = new StreamWriter(settings.OutputPath, false))
            {
                output.WriteLine("Run started {0}", DateTime.Now);
                output.WriteLine();
                output.WriteLine("Settings: ");
                output.WriteLine(settings);
                output.WriteLine();
            }

            // run setup proc
            if (settings.SetupProcName != null)
            {
                using (var conn = OpenConnection())
                {
                    Console.WriteLine("Running setup proc {0}", settings.SetupProcName);
                    Console.WriteLine();
                    conn.ExecuteNonQuery(settings.SetupProcName);
                }
            }

            // run test
            var runner = new TestRunner(settings);
            Console.WriteLine();
            Console.WriteLine("Running test {0} with {1} inserts", settings.RunName, settings.Inserts);
            runner.Run();

            // write fragmentation summary
            using (var output = new StreamWriter(settings.OutputPath, true))
            using (var conn = OpenConnection())
            {
                output.WriteLine();
                output.WriteLine("Fragmentation:");
                var table = conn.ExecuteTable("GetFragmentation");
                table.Dump(output);
                Console.WriteLine();
            }

            Console.WriteLine("Run complete");
            Console.WriteLine();
            return 0;
        }

        private static SqlConnection OpenConnection()
        {
            var conn = new SqlConnection(ConfigurationManager.AppSettings["connString"]);
            conn.Open();
            return conn;
        }
    }
}
