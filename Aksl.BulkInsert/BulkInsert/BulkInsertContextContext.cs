using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aksl.BulkInsert
{
    public class BulkInsertContextContext
    {
        /// <summary>
        /// The exception that occured in Load.
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// If true, the exception will not be rethrown.
        /// </summary>
        public bool Ignore { get; set; } = true;

        //执行时间
        public TimeSpan ExecutionTime { get; set; }

        public int MessageConunt { get; set; }
    }

    /// <summary>
    /// BulkInsertContextContext
    /// </summary>
    public class BulkInsertContextContext< T>
    {
        public IEnumerable<T> Result { get; set; }

        /// <summary>
        /// The exception that occured in Load.
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// If true, the exception will not be rethrown.
        /// </summary>
        public bool Ignore { get; set; } = true;

        //执行时间
        public TimeSpan ExecutionTime { get; set; }

        public int MessageConunt { get; set; }

       // public Func<IEnumerable<TMessage>, TResult[]> Handler { get; set; }
    }
}