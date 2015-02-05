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

        public string RunName { get; set; }

        [Required]
        [Range(1, int.MaxValue)]
        public long Inserts { get; set; }

        [Required]
        public int InsertThreads { get; set; }
        
        [Required]
        public int InsertBatchSize { get; set; }

        public int InsertBatchDelayMs { get; set; }

        public bool UseTransactionForInsertBatch { get; set; }

        [Required]
        public int SelectThreads { get; set; }

        public int SelectDelayMs { get; set; }

        public string SetupProcName { get; set; }

        [Required]
        public string InsertProcName { get; set; }

        [Required]
        public string SelectProcName { get; set; }

        /// <summary>
        /// Recompile select after N executions.
        /// </summary>
        public int RecompileSelectAfter { get; set; }

        public int QueryTimeoutSeconds { get; set; }

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
