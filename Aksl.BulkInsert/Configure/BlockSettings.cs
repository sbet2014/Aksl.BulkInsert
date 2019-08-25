using System;

namespace Aksl.BulkInsert.Configuration
{
    public class BlockSettings
    {
        public static BlockSettings Default => new BlockSettings();

        public BlockSettings()
        {
            BlockCount = Environment.ProcessorCount * 4;//块数
            MinPerBlock = 20;//至少有一块8条
            MaxPerBlock = 200;//至多
            MaxDegreeOfParallelism = Environment.ProcessorCount * 2;//并行数
        }

        public int BlockCount { get; set; }

        public int MinPerBlock { get; set; }

        public int MaxPerBlock { get; set; }

        public int MaxDegreeOfParallelism { get; set; }
    }
}