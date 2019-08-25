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
    public class PipeBulkInserter<TMessage, TResult> : IPipeBulkInserter<TMessage, TResult>
    {
        #region Members
        private AsyncLock _mutexReadMessage;

        protected Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> _insertHandler;

        private PipeSettings _pipeSettings;
        private BlockSettings _blockSettings;

        protected ILoggerFactory _loggerFactory;
        protected ILogger _logger;
        #endregion

        #region Constructors
        /// <summary>
        /// Constructor
        /// </summary>
        public PipeBulkInserter(IOptions<PipeSettings> pipeOptions = null, IOptions<BlockSettings> blockOptions = null, ILoggerFactory loggerFactory = null) =>
                          InitializeBulkInserter(null, pipeOptions?.Value, blockOptions?.Value, loggerFactory);

        public PipeBulkInserter(Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> insertHandler , IOptions<PipeSettings> pipeOptions = null, IOptions<BlockSettings> blockOptions = null, ILoggerFactory loggerFactory = null) =>
                          InitializeBulkInserter(insertHandler, pipeOptions?.Value, blockOptions?.Value, loggerFactory);
        public PipeBulkInserter(Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> insertHandler, PipeSettings pipeSettings = null, BlockSettings blockSettings = null, ILoggerFactory loggerFactory = null) =>
                          InitializeBulkInserter(insertHandler, pipeSettings, blockSettings, loggerFactory);

        protected void InitializeBulkInserter(Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> insertHandler, PipeSettings pipeSettings, BlockSettings blockSettings, ILoggerFactory loggerFactory)
        {
            _insertHandler = insertHandler ;

            _pipeSettings = pipeSettings ?? PipeSettings.Default;
            _blockSettings = blockSettings ?? BlockSettings.Default;

            _mutexReadMessage = new AsyncLock();

            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

            _logger = loggerFactory?.CreateLogger(nameof(PipeBulkInserter<TMessage, TResult>));
        }
        #endregion

        #region Properties
        public Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> InsertHandler
        {
            get => _insertHandler ?? throw new ArgumentNullException(nameof(_insertHandler));
            set => _insertHandler = value;
        }

        public Action<BulkInsertContextContext<TResult>> OnInsertCallBack
        {
            get;
            set;
        }
        #endregion

        #region  IPipeBulkInserter
        public async Task<TResult[]> BulkInsertAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default)
        {
            #region Initialize Method
            if (!(messages?.Any()).HasValue)
            {
                return default;
            }

            int messageCount = messages.Count();
            var allResults = new List<TResult>(messages.Count());
            var context = new BulkInsertContextContext<TResult>() { MessageConunt = messageCount };
            TimeSpan maxExecutionTime = TimeSpan.Zero; //花去的最长时间
            #endregion

            try
            {
                #region Block Method
                int blockCount = _blockSettings.BlockCount;//块数
                int minPerBlock = _blockSettings.MinPerBlock;//至少
                int maxPerBlock = _blockSettings.MaxPerBlock;//至多
                int maxDegreeOfParallelism = _blockSettings.MaxDegreeOfParallelism;//并行数

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

                #region Pipe Method
                var sw = Stopwatch.StartNew();

                for (int i = 0; i < blockMessages.Count(); i++)
                {
                    sw.Restart();

                    var currentPipe = CreateCurrentPipe();

                    var readask = DoReadAsync(currentPipe.Reader, allResults);
                    var writeTask = DoWriteAsync(currentPipe.Writer, blockMessages[i], _pipeSettings.MinAllocBufferSize);

                    await writeTask;
                    await readask;

                    currentPipe = null;

                    maxExecutionTime = maxExecutionTime.Ticks < sw.Elapsed.Ticks ? sw.Elapsed : maxExecutionTime;
                    sw.Reset();
                }
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

            return allResults?.ToArray();
        }
        #endregion

        #region Create PipeMethod
        private Pipe CreateCurrentPipe()
        {
            var memoryPool = MemoryPoolFactory.Create();
            var readerScheduler = PipeScheduler.ThreadPool;

            var writerScheduler = new IOQueue();

            var pipeOptions = PipeHelper.GetPipeOptions(writerScheduler, readerScheduler, memoryPool,
                                                        _pipeSettings.PauseWriterThreshold, _pipeSettings.ResumeWriterThreshold,
                                                         _pipeSettings.MinimumSegmentSize, _pipeSettings.UseSynchronizationContext);
            var pipe = new Pipe(pipeOptions);
            return pipe;
        }
        #endregion

        #region Write Methods
        private async ValueTask DoWriteAsync(PipeWriter writer, TMessage[] messages, int allocBufferSize = 512)
        {
            Exception error = null;

            try
            {
                await ProcessWriteAsync(writer, messages, allocBufferSize);
            }
            catch (Exception ex)
            {
                error = ex;
            }
            finally
            {
                writer.Complete(error);
            }
        }

        private async ValueTask ProcessWriteAsync(PipeWriter writer, TMessage[] messages, int allocBufferSize)
        {
            #region Methods
            //PipeTextWriter pipeTextWriter = PipeTextWriter.Create(writer, Encoding.UTF8, writeBOM: false, closeWriter: false, autoFlush: false);
            //pipeTextWriter.NewLine = new string('\n', 1);

            //foreach (var msg in messages)
            //{
            //    var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(msg));

            //    var charArray = bytes.Select(b => (char)b).ToArray();
            //    await pipeTextWriter.WriteLineAsync(charArray, 0, charArray.Length);
            //}

            //var flushTask = writer.FlushAsync();
            //if (!flushTask.IsCompleted)
            //{
            //    await flushTask;
            //}

            //await flushTask;
            #endregion

            #region Methods
            //int totalWriteBytes = 0;
            foreach (var msg in messages)
            {
                var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(msg));

                var destArray = new byte[bytes.Length + 1];
                Array.Copy(bytes, destArray, bytes.Length);
                destArray[bytes.Length] = (byte)'\n';

                var memory = writer.GetMemory(allocBufferSize);

                var isArray = MemoryMarshal.TryGetArray<byte>(memory, out var arraySegment);
                Debug.Assert(isArray);

                destArray.AsMemory().CopyTo(arraySegment);

                writer.Advance(destArray.Count());

                //   _logger.LogInformation($"Write To Pipe Bytes :{destArray.Length},now:{DateTime.Now.TimeOfDay}");

                // totalWriteBytes += destArray.Count();
            }

            //writer.Advance(totalWriteBytes);

            var flushTask = writer.FlushAsync();
            if (!flushTask.IsCompleted)
            {
                await flushTask;
            }

            await flushTask;
            #endregion
        }
        #endregion

        #region Read Methods
        private async ValueTask DoReadAsync(PipeReader reader, List<TResult> allResults)
        {
            Exception error = null;

            try
            {
                await ProcessReadAsync(reader, allResults);
            }
            catch (Exception ex)
            {
                error = ex;
            }
            finally
            {
                // We're done writing
                reader.Complete(error);
            }
        }

        private async ValueTask ProcessReadAsync(PipeReader reader, List<TResult> allResults)
        {
            bool isEmpty = false;
            while (!isEmpty)
            {
                // await some data being available
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                if (result.IsCanceled)
                {
                    break;
                }

                var end = buffer.End;
                var isCompleted = result.IsCompleted;
                if (!buffer.IsEmpty)
                {
                    var isSingleSegment = buffer.IsSingleSegment;
                    var length = buffer.Length;
                    using (await _mutexReadMessage.LockAsync())
                    {
                        var messageList = buffer.ToObjects<TMessage>();

                        var resuls = await InsertHandler?.Invoke(messageList);

                        if ((resuls?.Any()).HasValue)
                        {
                            allResults.AddRange(resuls);
                        }
                    }
                }

                // tell the pipe that we used everything
                reader.AdvanceTo(buffer.End);

                if (isCompleted)
                {
                    break;
                }

                isEmpty = true;
            }
        }
        #endregion

        #region  IDisposable
        public void Dispose()
        {
            try
            {
                _mutexReadMessage = null;
                _insertHandler = null;
            }
            catch (Exception ex)
            {
                _logger?.LogError($"Error when dispose: '{ex.Message}'");
            }
        }
        #endregion
    }
}