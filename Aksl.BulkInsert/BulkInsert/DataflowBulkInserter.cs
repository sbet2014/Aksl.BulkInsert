using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

using Aksl.Pipeline;
using Aksl.Concurrency;
using Aksl.BulkInsert.Configuration;

namespace Aksl.BulkInsert
{
    /// <summary>
    /// Bulk Inserter
    /// </summary>
    public class DataflowBulkInserter<TMessage, TResult> : IDataflowBulkInserter<TMessage, TResult>
    {
        #region Members
        private AsyncLock _mutexResult;

        protected Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> _insertHandler;

        private BlockSettings _blockSettings;

        protected ILoggerFactory _loggerFactory;
        protected ILogger _logger;
        #endregion

        #region Constructors
        /// <summary>
        /// Constructor
        /// </summary>
        public DataflowBulkInserter(IOptions<BlockSettings> blockOptions = null, ILoggerFactory loggerFactory = null) =>
                                                       InitializeBulkInserter(null, blockOptions?.Value, loggerFactory);

        public DataflowBulkInserter(Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> insertHandler, BlockSettings blockSettings = null, ILoggerFactory loggerFactory = null) =>
                                                        InitializeBulkInserter(insertHandler, blockSettings, loggerFactory);

        protected void InitializeBulkInserter(Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> insertHandler, BlockSettings blockSettings, ILoggerFactory loggerFactory)
        {
            _insertHandler = insertHandler;

            _blockSettings = blockSettings ?? BlockSettings.Default;

            _mutexResult = new AsyncLock();

            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

            _logger = loggerFactory.CreateLogger(nameof(DataflowBulkInserter<TMessage, TResult>));
        }
        #endregion

        #region Properties
        public Action<BulkInsertContextContext<TResult>> OnInsertCallBack
        {
            get;
            set;
        }

        public Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> InsertHandler
        {
            get => _insertHandler ?? throw new ArgumentNullException(nameof(_insertHandler));
            set => _insertHandler = value;
        }
        #endregion

        #region BulkInsert Method
        /// <summary>
        ///  Bulk Insert
        /// </summary>
        /// <param name="messages">Messages</param>
        /// <returns>Task</returns>
        public async Task<IEnumerable<TResult>> BulkInsertAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default)
        {
            #region Initialize Method
            if (!(messages?.Any()).HasValue)
            {
                return default;
            }

            var headBlock = default(BufferBlock<TMessage[]>);
            var writeBlocks = default(List<ActionBlock<TMessage[]>>);

            int messageCount = messages.Count();
            var allResults = new List<TResult>(messages.Count());
            var context = new BulkInsertContextContext<TResult>() { MessageConunt = messageCount };
            TimeSpan maxExecutionTime = TimeSpan.Zero; //花去的最长时间
            #endregion

            try
            {
                #region Block Methods
                //int blockCount = Environment.ProcessorCount * 2;//块数
                //int minPerBlock = 100;//至少有一块20条
                //int maxPerBlock = 200;//至多
                //int maxDegreeOfParallelism = Environment.ProcessorCount * 2;//并行数

                int blockCount = _blockSettings.BlockCount;//块数
                int minPerBlock = _blockSettings.MinPerBlock;//至少
                int maxPerBlock = _blockSettings.MaxPerBlock;//至多
                int maxDegreeOfParallelism = _blockSettings.MaxDegreeOfParallelism;//并行数

                //分块
                //var blockInfos = BlockHelper.MacthBlockInfoDown(blockCount, messageCount, minPerBlock);
                int[] blockInfos = default;
                if (messageCount < (blockCount * maxPerBlock))
                {
                    //分块
                    blockInfos = BlockHelper.MacthBlockInfoDown(blockCount, messageCount, minPerBlock);
                }
                else
                {
                    blockInfos = BlockHelper.MacthBlockInfoUp(blockCount, messageCount, maxPerBlock);
                }

                var blockMessages = BlockHelper.GetMessageByBlockInfo<TMessage>(blockInfos, messages.ToArray()).ToList();
                #endregion

                #region Send Method
                //创建通道
                CreateBlockers(maxDegreeOfParallelism, blockInfos, blockMessages);

                foreach (var msg in blockMessages)
                {
                    await headBlock.SendAsync(msg)
                       .ConfigureAwait(continueOnCapturedContext: false);
                }

                #region Parallel
                //Parallel.ForEach(blockMessages, async msg =>
                //{
                //    try
                //    {
                //        await headBlock.SendAsync(msg);
                //    }
                //    catch (Exception ex)
                //    {
                //        _logger.LogError($"exception: {ex.Message} when block send message");
                //    }
                //});
                #endregion

                headBlock.Complete();
                await headBlock.Completion.ContinueWith(_ =>
                {
                    //  _logger.LogInformation($"{1} message was send by {headBlock.GetType()}.");

                    foreach (var wb in writeBlocks)
                    {
                        try
                        {
                            wb.Complete();
                            wb.Completion.Wait();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError($"Error while {nameof(wb)} waiting: {ex.Message}");
                        }
                    }
                });
                #endregion

                #region Finish Method
                context.Result = allResults;
                context.ExecutionTime = maxExecutionTime;
                OnInsertCallBack?.Invoke(context);
                #endregion
            }
            catch (Exception ex)
            {
                #region Methods
                _logger?.LogError($"Error when insert message: '{ex.ToString()}'");

                context.Exception = ex;
                OnInsertCallBack?.Invoke(context);
                if (!context.Ignore)
                {
                    throw;
                }
                #endregion
            }

            void CreateBlockers(int maxDegreeOfParallelism, int[] blockInfos, IList<TMessage[]> blockMessages)
            {
                #region Channel Methods
                int boundedCapacity = blockInfos.Sum(b => b);

                headBlock = new BufferBlock<TMessage[]>();
                writeBlocks = new List<ActionBlock<TMessage[]>>(blockInfos.Count());

                //限制容量,做均衡负载
                for (int i = 0; i < blockInfos.Count(); i++)
                {
                    #region Method
                    var writeBlock = new ActionBlock<TMessage[]>(async (blockDatas) =>
                    {
                        var sw = Stopwatch.StartNew();
                        var results = new List<TResult>();

                        try
                        {
                            using (await _mutexResult.LockAsync())
                            {
                                var resuls = await InsertHandler?.Invoke(blockDatas);
                                if ((resuls?.Any()).HasValue)
                                {
                                    allResults.AddRange(resuls);

                                    maxExecutionTime = maxExecutionTime.Ticks < sw.Elapsed.Ticks ? sw.Elapsed : maxExecutionTime;

                                    //_logger
                                    //   .LogInformation($"ExecutionTime={sw.Elapsed},ThreadId={Thread.CurrentThread.ManagedThreadId},Count=\"{resuls?.Count()}\"");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            context.Exception = ex;
                        }
                    },
                    new ExecutionDataflowBlockOptions()
                    {
                        BoundedCapacity = blockInfos[i],//限制容量,做均衡负载
                        CancellationToken = cancellationToken
                    });

                    writeBlocks.Add(writeBlock);
                    #endregion
                }

                for (int i = 0; i < writeBlocks.Count(); i++)
                {
                    if (writeBlocks[i] is ITargetBlock<TMessage[]>)
                    {
                        headBlock.LinkTo(writeBlocks[i], (msgs) =>
                        {
                            return msgs?.Count() > 0;
                        });
                    }
                }

                #endregion
            }

            return allResults;
        }
        #endregion
    }
}