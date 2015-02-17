using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace SprocHammer
{
    public class TestRunner
    {
        private const double TicksPerSecond = 10000.0 * 1000.0;
        private readonly static Random _random = new Random();

        private readonly Stopwatch _stopwatch = new Stopwatch();
        private readonly TestRunSettings _settings;

        private long _inserts;
        private long _insertTicks;
        private double _insertSecsAverage;
        private long _selects;
        private long _selectTicks;
        private double _selectSecsAverage;
        private long _timeouts;
        private TextWriter _output;

        public TestRunner(TestRunSettings settings)
        {
            _settings = settings;
        }

        public TimeSpan Run()
        {
            File.AppendAllText(_settings.OutputPath, "time\tinserts\tsecPerIns\tinsSecTotal\tselects\tsecPerSel\tselSecTot\ttimeouts" + Environment.NewLine);

            using (var textWriter = new StreamWriter(_settings.OutputPath, true))
            {
                _output = TextWriter.Synchronized(textWriter);

                var tasks = new ConcurrentBag<Task>();
                var tokenSource = new CancellationTokenSource();
                var cancellationToken = tokenSource.Token;

                lock (_stopwatch)
                {
                    _stopwatch.Start();
                }

                Console.WriteLine("Starting {0} insert threads with {1}ms cycle time",
                    _settings.InsertThreads, _settings.InsertBatchDelayMs);
                for (int i = 0; i < _settings.InsertThreads; i++)
                {
                    tasks.Add(
                        Task.Run(() => RunWithErrorLogging(GenerateData), 
                        cancellationToken));
                }

                Console.WriteLine("Starting {0} select threads with {1}ms cycle time",
                    _settings.SelectThreads, _settings.SelectDelayMs);
                for (int i = 0; i < _settings.SelectThreads; i++)
                {
                    tasks.Add(
                        Task.Run(() => RunWithErrorLogging(SelectData),
                        cancellationToken));
                }

                var sampleTimer = new Timer(_ => SampleData(), null, 1000, 1000);
                
                try
                {
                    Console.WriteLine("Waiting for tasks to complete");
                    Task.WaitAll(tasks.ToArray());
                }
                catch (AggregateException ex)
                {
                    Console.WriteLine("Something when shutting down:");
                    foreach (var innerEx in ex.InnerExceptions)
                    {
                        PrintException(innerEx);
                    } 
                }

                lock (_stopwatch)
                {
                    _stopwatch.Stop();
                }

                Console.WriteLine("Waiting for capture task to stop");
                sampleTimer.Dispose();
                
                Thread.Sleep(100);

                // write results to file and stdout
                
                _output.WriteLine();
                WriteResults(_output);
                
                Console.WriteLine();
                Console.WriteLine("Final results:");
                WriteResults(Console.Out);

                return _stopwatch.Elapsed;
            }
        }

        private void WriteResults(TextWriter output)
        {
            double elapsedSecs;
            lock (_stopwatch)
            {
                elapsedSecs = _stopwatch.Elapsed.TotalSeconds;
            }
            output.WriteLine();
            output.WriteLine("  Elapsed sec: {0:F2}", elapsedSecs);
            output.WriteLine("  Inserts: {0}", Interlocked.Read(ref _inserts));
            output.WriteLine("  Sec/insert: {0:F8}", Interlocked.CompareExchange(ref _insertSecsAverage, 0, -1));
            output.WriteLine("  Selects: {0}", Interlocked.Read(ref _selects));
            output.WriteLine("  Sec/select: {0:F4}", Interlocked.CompareExchange(ref _selectSecsAverage, 0, -1));
            output.WriteLine("  Timeouts: {0}", Interlocked.Read(ref _timeouts));
            output.WriteLine();
        }

        /// <summary>
        /// Run data generation loop.
        /// </summary>
        private void GenerateData()
        {
            Random random;
            lock (_random)
            {
                random = new Random(_random.Next());
            }
            
            Thread.Sleep(random.Next(200));

            using (var conn = CreateConnection())
            {
                SqlTransaction transaction = null;

                var stopwatch = new Stopwatch();
                conn.Open();
                while (true)
                {
                    if (_settings.UseTransactionForInsertBatch)
                    {
                        transaction = conn.BeginTransaction();
                    }
                    stopwatch.Restart();
                    for (int i = 0; i < _settings.InsertBatchSize; i++)
                    {
                        if (EnoughAlready())
                        {
                            return;
                        }

                        try
                        {
                            conn.ExecuteNonQuery(_settings.InsertProcName, _settings.QueryTimeoutSeconds, transaction);
                        }
                        catch (SqlException ex)
                        {
                            if (ex.Message.ToUpperInvariant().Contains("TIMEOUT"))
                            {
                                Interlocked.Increment(ref _timeouts);
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                        transaction = null;
                    }
                    stopwatch.Stop();
                    Interlocked.Add(ref _inserts, _settings.InsertBatchSize);
                    Interlocked.Add(ref _insertTicks, stopwatch.ElapsedTicks);

                    var average = Interlocked.CompareExchange(ref _insertSecsAverage, 0, -1);
                    if (average == 0)
                    {
                        Interlocked.Exchange(ref _insertSecsAverage, stopwatch.Elapsed.TotalSeconds / _settings.InsertBatchSize);
                    }
                    else
                    {
                        // weighted average of recent values
                        Interlocked.Exchange(ref _insertSecsAverage, average * 0.75 + stopwatch.Elapsed.TotalSeconds / _settings.InsertBatchSize * 0.25);
                    }

                    Thread.Sleep(_settings.InsertBatchDelayMs);
                }
            }
        }

        /// <summary>
        /// Run data select loop.
        /// </summary>
        private void SelectData()
        {
            Thread.Sleep(_random.Next(200));

            var stopwatch = new Stopwatch();
            using (var conn = CreateConnection())
            {
                conn.Open();
                while (true)
                {
                    stopwatch.Restart();
                    if (EnoughAlready())
                    {
                        return;
                    }

                    try
                    {
                        conn.ExecuteNonQuery(_settings.SelectProcName, _settings.QueryTimeoutSeconds);
                    }
                    catch (SqlException ex)
                    {
                        if (ex.Message.ToUpperInvariant().Contains("TIMEOUT"))
                        {
                            Interlocked.Increment(ref _timeouts);
                        }
                        else
                        {
                            throw;
                        }
                    }
                    stopwatch.Stop();
                    long selects = Interlocked.Increment(ref _selects);
                    Interlocked.Add(ref _selectTicks, stopwatch.ElapsedTicks);
                    
                    var average = Interlocked.CompareExchange(ref _selectSecsAverage, 0, -1);
                    if (average == 0)
                    {
                        Interlocked.Exchange(ref _selectSecsAverage, stopwatch.Elapsed.TotalSeconds);
                    }
                    else
                    {
                        // weighted average of recent values
                        Interlocked.Exchange(ref _selectSecsAverage, average*0.75 + stopwatch.Elapsed.TotalSeconds*0.25);
                    }

                    if (_settings.RecompileSelectAfter > 0 && selects % _settings.RecompileSelectAfter == 0)
                    {
                        var cmd = new SqlCommand("sp_recompile", conn)
                        {
                            CommandType = CommandType.Text
                        };
                        cmd.Parameters.AddWithValue("objname", _settings.InsertProcName);
                    }

                    Thread.Sleep(_settings.SelectDelayMs);
                }
            }
        }

        /// <summary>
        /// Start loop for sampling results.
        /// </summary>
        private void SampleData()
        {
            if (EnoughAlready())
            {
                return;
            }

            double elapsedSec;
            lock (_stopwatch)
            {
                elapsedSec = _stopwatch.Elapsed.TotalSeconds;
            }

            long inserts = Interlocked.Read(ref _inserts);
            long insertTicks = Interlocked.Read(ref _insertTicks);
            long selects = Interlocked.Read(ref _selects);
            long selectTicks = Interlocked.Read(ref _selectTicks);
            long timeouts = Interlocked.Read(ref _timeouts);
            double selectAvg = Interlocked.CompareExchange(ref _selectSecsAverage, 0, -1);
            double insertAvg = Interlocked.CompareExchange(ref _insertSecsAverage, 0, -1);

            _output.WriteLine("{0:F2}\t{1}\t{2:F8}\t{3:F6}\t{4}\t{5:F6}\t{6:F6}\t{7}",
                elapsedSec,
                inserts,
                insertAvg,
                insertTicks/TicksPerSecond,
                selects,
                selectAvg,
                selectTicks/TicksPerSecond,
                timeouts);

            if (((int) elapsedSec) % 100 == 0)
            {
                WriteResults(Console.Out);
            }

            _output.Flush();
        }

        private bool EnoughAlready()
        {
            if (Interlocked.Read(ref _inserts) >= _settings.Inserts)
            {
                return true;
            }
            return false;
        }

        private void RunWithErrorLogging(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                PrintException(ex);
                throw;
            }
        }

        private static void PrintException(Exception ex)
        {
            Console.WriteLine("({0}) {1}", ex.GetType().Name, ex.Message);
        }

        private SqlConnection CreateConnection()
        {
            return new SqlConnection(ConfigurationManager.AppSettings["connString"]);
        }
    }
}