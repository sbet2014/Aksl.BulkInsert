using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Aksl.Concurrency;

namespace Aksl.BulkInsert
{
    /// <summary>
    /// Bulk Inserter
    /// </summary>
    public class NoResultBulkInserter<TMessage> : INoResultBulkInserter<TMessage>
    {
        #region Members
        private AsyncLock _mutexResult;

        protected Func<IEnumerable<TMessage>, Task> _insertHandler;

        protected ILoggerFactory _loggerFactory;
        protected ILogger _logger;
        #endregion

        #region Constructors
        /// <summary>
        /// Constructor
        /// </summary>
        public NoResultBulkInserter(Func<IEnumerable<TMessage>, Task> insertHandler, ILoggerFactory loggerFactory = null) =>
                                                              InitializeBulkInserter(insertHandler, loggerFactory);

        protected void InitializeBulkInserter(Func<IEnumerable<TMessage>, Task> insertHandler, ILoggerFactory loggerFactory)
        {
            _insertHandler = insertHandler ?? throw new ArgumentNullException(nameof(insertHandler));

            _mutexResult = new AsyncLock();

            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

            _logger = loggerFactory.CreateLogger(nameof(NoResultBulkInserter<TMessage>));
        }
        #endregion

        #region Properties
        public Action<BulkInsertContextContext> OnInsertCallBack
        {
            get;
            set;
        }

        public Func<IEnumerable<TMessage>, Task> InsertHandler
        {
            get => _insertHandler ?? throw new ArgumentNullException(nameof(_insertHandler));
            set => _insertHandler = value;
        }
        #endregion

        #region SendBatch Methods
        /// <summary>
        /// Send Message to Queue
        /// </summary>
        /// <param name="messages">Messages</param>
        /// <returns>Task</returns>
        public async Task BulkInserterAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default)
        {
            if (!(messages?.Any()).HasValue)
            {
                return ;
            }

            var headBlock = default(BufferBlock<TMessage[]>);
            var writeBlocks = default(List<ActionBlock<TMessage[]>>);

            int messageCount = messages.Count();
            var context = new BulkInsertContextContext() { MessageConunt = messageCount };
            TimeSpan maxExecutionTime = TimeSpan.Zero; //花去的最长时间

            try
            {
                #region Block Methods
                int blockCount = Environment.ProcessorCount * 2;//块数
                int minPerBlock = 1000;//至少有一块20条
                int maxPerBlock = 2000;//至多
                int maxDegreeOfParallelism = Environment.ProcessorCount * 2;//并行数

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

                #region Inser Methods
                //创建通道
                CreateBlockers(maxDegreeOfParallelism, blockInfos, blockMessages);

                foreach (var msg in blockMessages)
                {
                    await headBlock.SendAsync(msg)
                       .ConfigureAwait(continueOnCapturedContext: false);
                }

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

                headBlock.Complete();
                await headBlock.Completion.ContinueWith(_ =>
                {
                    //  _logger.LogInformation($"{1} message was send by {headBlock.GetType()}.");

                    foreach (var wb in writeBlocks)
                    {
                        wb.Complete();
                        wb.Completion.Wait();
                    }
                });

                context.ExecutionTime = maxExecutionTime;
                OnInsertCallBack?.Invoke(context);
                #endregion
            }
            catch (Exception ex)
            {
                #region Methods
                _logger?.LogError($"Error when send message: '{ex.ToString()}'");

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

                        try
                        {
                            using (await _mutexResult.LockAsync())
                            {
                                await InsertHandler?.Invoke(blockDatas);

                                maxExecutionTime = maxExecutionTime.Ticks < sw.Elapsed.Ticks ? sw.Elapsed : maxExecutionTime;

                                //_logger
                                //   .LogInformation($"ExecutionTime={sw.Elapsed},ThreadId={Thread.CurrentThread.ManagedThreadId},Count=\"{resuls?.Count()}\"");
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

           // return allResults?.ToArray();
        }
        #endregion
    }
}