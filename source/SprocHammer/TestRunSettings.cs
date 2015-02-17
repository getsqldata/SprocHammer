using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using Newtonsoft.Json;

namespace SprocHammer
{
    public class TestRunSettings
    {
        public static TestRunSettings DeserializeJson(string content)
        {
            return JsonConvert.DeserializeObject<TestRunSettings>(content);
        }

        public TestRunSettings()
        {
            Inserts = 1000;
            InsertBatchSize = 10;
            InsertThreads = 1;
            SelectThreads = 1;
            QueryTimeoutSeconds = 30;
        }

        /// <summary>
        /// Name used to identify the run. Will be taken from settings file name if not specified.
        /// </summary>
        public string RunName { get; set; }

        /// <summary>
        /// Number of inserts to perform before stopping.
        /// </summary>
        [Required]
        [Range(1, int.MaxValue)]
        public long Inserts { get; set; }

        /// <summary>
        /// Number of concurrent insert threads.
        /// </summary>
        [Required]
        public int InsertThreads { get; set; }
        
        /// <summary>
        /// Number of inserts to run in one batch.
        /// </summary>
        [Required]
        public int InsertBatchSize { get; set; }

        /// <summary>
        /// Delay to use between insert batches (ms).
        /// </summary>
        public int InsertBatchDelayMs { get; set; }

        /// <summary>
        /// If true, run each insert batch within a unique transaction.
        /// </summary>
        public bool UseTransactionForInsertBatch { get; set; }

        /// <summary>
        /// Number of concurrent select threads.
        /// </summary>
        [Required]
        public int SelectThreads { get; set; }

        /// <summary>
        /// Delay between each select execution.
        /// </summary>
        public int SelectDelayMs { get; set; }

        /// <summary>
        /// Name of proc to call at beginning to set up the run.
        /// </summary>
        public string SetupProcName { get; set; }

        /// <summary>
        /// Name of the proc to use for inserting data.
        /// </summary>
        [Required]
        public string InsertProcName { get; set; }

        /// <summary>
        /// Name of the proc to use for finding data.
        /// </summary>
        [Required]
        public string SelectProcName { get; set; }

        /// <summary>
        /// Force SQL Server to recompile select query after N executions.
        /// If zero, no force recompilation will be done.
        /// </summary>
        public int RecompileSelectAfter { get; set; }

        /// <summary>
        /// Timeout for query execution (s).
        /// </summary>
        public int QueryTimeoutSeconds { get; set; }

        /// <summary>
        /// Output path for log, otherwise use current directory.
        /// </summary>
        public string OutputPath { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }

        public bool Validate(TextWriter output)
        {
            var context = new ValidationContext(this, null, null);
            var results = new List<ValidationResult>();
            if (Validator.TryValidateObject(
                this, context, results, true
                ))
            {
                return true;
            }
            output.WriteLine("Settings object is invalid:");
            foreach (var result in results)
            {
                output.WriteLine("{0}, {1}", string.Join(", ", result.MemberNames), result.ErrorMessage);
            }
            return false;
        }
    }
}
