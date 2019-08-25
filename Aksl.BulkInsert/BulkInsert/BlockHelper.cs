using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aksl.BulkInsert
{
    public class BlockHelper
    {
        public static IEnumerable<T[]> GetBlockMessages<T>(int blockCount, T[] messages)
        {
            var blockMessages = new List<T[]>(blockCount);

            int messagesCount = messages.Count();
            //int blockSize = messagesCount / blockCount;//每一块的数量
            //blockSize = blockSize == 0 ? 1 : blockSize;
            int[] blockInfos = GetBlockInfo(blockCount, messagesCount);//分成x块

            int startPositon = 0;
            for (int i = 0; i < blockCount; i++)
            {
                //int startPositon = i * blockSize;
                var messagesInChunk = new T[blockInfos[i]];
                for (int j = 0; j < blockInfos[i]; j++)
                {
                    int positon = startPositon + j;
                    var msg = messages[positon];
                    messagesInChunk[j] = msg;
                }
                startPositon += blockInfos[i];
                blockMessages.Add(messagesInChunk);
            }

            return blockMessages;
        }

        public static int[] GetBlockInfo(int blockCount, int totalCount)
        {
            int[] chunks = new int[blockCount];//分成x块
            int chunkSize = totalCount / blockCount;//每一块的数量
            int chunkMore = totalCount % blockCount;//余下的数量

            if (chunkSize == 0)//小于x
            {
                for (int i = 0; i < chunkMore; i++)
                {
                    chunks[i] = 1;
                }
                chunkSize = 1;
            }
            else
            {
                for (int i = 0; i < blockCount; i++)
                {
                    chunks[i] = chunkSize;
                }
                if (chunkMore > 0)
                {
                    //chunks[blockCount - 1] = chunks[blockCount - 1] + chunkMore;
                    for (int i = 0; i < blockCount && chunkMore > 0; i++)
                    {
                        chunks[i] = chunks[i] + 1;
                        chunkMore--;
                    }
                }
            }

            return chunks;
        }

        public static int[] MacthBlockInfo(int blockCount, int totalCount)
        {
            int fac = blockCount;
            var blockInfos = GetBlockInfo(fac, totalCount);

            while (blockInfos.Any(b => b <= 0))
            {
                fac = fac / 2;
                blockInfos = GetBlockInfo(fac, totalCount);
            };

            return blockInfos;
        }

        public static int[] MacthBlockInfoDown(int blockCount, int totalCount, int minnumPerBlock)
        {
            int fac = blockCount;
            var blockInfos = GetBlockInfo(fac, totalCount);

            bool isLittle = blockInfos.Any(b => b <= 0) || !blockInfos.Any(b => b >= minnumPerBlock);//至少有一个

            while (isLittle && fac > 1)
            {
                fac = fac - 1;
                blockInfos = GetBlockInfo(fac, totalCount);
                isLittle = blockInfos.Any(b => b <= 0) || !blockInfos.Any(b => b >= minnumPerBlock);
                if (isLittle && fac == 2)
                {
                    if (blockInfos.Count() == 2 && blockInfos.Sum(b => b) > minnumPerBlock)
                        isLittle = false;
                }
            };

            return blockInfos;
        }

        public static int[] MacthBlockInfoUp(int blockCount, int totalCount, int maxnumPerBlock)
        {
            int fac = blockCount;
            var blockInfos = GetBlockInfo(fac, totalCount);

            bool isLarge = blockInfos.Any(b => b > maxnumPerBlock);

            while (isLarge)
            {
                fac = fac + 1;
                blockInfos = GetBlockInfo(fac, totalCount);
                isLarge = blockInfos.Any(b => b > maxnumPerBlock);
            };

            return blockInfos;
        }

        public static IEnumerable<T[]> MacthBlockMessage<T>(int blockCount, T[] messages)
        {
            int totalCount = messages.Count();
            int fac = blockCount;
            var blockInfos = MacthBlockInfo(blockCount, totalCount);

            var blockMessages = new List<T[]>(blockCount);

            int startPositon = 0;
            for (int i = 0; i < blockInfos.Count(); i++)
            {
                var messagesInChunk = new T[blockInfos[i]];
                for (int j = 0; j < blockInfos[i]; j++)
                {
                    int positon = startPositon + j;
                    var msg = messages[positon];
                    messagesInChunk[j] = msg;
                }
                startPositon += blockInfos[i];
                blockMessages.Add(messagesInChunk);
            }

            return blockMessages;
        }

        public static IEnumerable<T[]> MacthBlockMessage<T>(int blockCount, T[] messages, int minnumPerBlock)
        {
            int totalCount = messages.Count();
            var blockInfos = MacthBlockInfoDown(blockCount, totalCount, minnumPerBlock);

            var blockMessages = GetMessageByBlockInfo<T>(blockInfos, messages);
            return blockMessages;
        }

        public static IEnumerable<T[]> GetMessageByBlockInfo<T>(int[] blockInfos, T[] messages)
        {
            var count = blockInfos.Sum(b => b);
            var blockMessages = new List<T[]>(count);

            int startPositon = 0;
            for (int i = 0; i < blockInfos.Count(); i++)
            {
                var msgScope = new T[blockInfos[i]];
                for (int j = 0; j < blockInfos[i]; j++)
                {
                    int positon = startPositon + j;
                    var msg = messages[positon];
                    msgScope[j] = msg;
                }
                startPositon += blockInfos[i];//重置起始位置
                blockMessages.Add(msgScope);
            }

            return blockMessages;
        }

        //api/todo/getpagedtodosasync?pageIndex=1&pageSize=10
        public static IEnumerable<string> GetBlockRequestUri(int[] blockInfos, string requestUri)
        {
            var uris = new List<string>(blockInfos.Count());

            int pageIndex = 0;
            for (int i = 0; i < blockInfos.Count(); i++)
            {
                int pageSize = blockInfos[i];
                for (int j = 0; j < blockInfos[i]; j++)
                {
                    int positon = pageIndex + j;
                }
                string uri = string.Format(requestUri, pageIndex, pageSize);
                pageIndex += blockInfos[i];//重置起始位置
                uris.Add(uri);
            }

            return uris;
        }
    }
}

