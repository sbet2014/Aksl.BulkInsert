using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aksl.BulkInsert
{
    #region IBulkInserter
    /// <summary>
    /// Bulk Inserter Interface
    /// </summary>
    public interface IDataflowBulkInserter<TMessage, TResult>
    {
        #region Properties
        Action<BulkInsertContextContext<TResult>> OnInsertCallBack { get; set; }

        Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> InsertHandler { get; set; }
        #endregion

        #region Bulk Insert Method
        Task<IEnumerable<TResult>> BulkInsertAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default);
        #endregion
    }
    #endregion

    #region IBulkInserter
    /// <summary>
    /// Bulk Inserter Interface
    /// </summary>
    public interface IDataflowPipeBulkInserter<TMessage, TResult>
    {
        #region Properties
        Action<BulkInsertContextContext<TResult>> OnInsertCallBack { get; set; }

        Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> InsertHandler { get; set; }
        #endregion

        #region Bulk Insert Method
        Task<IEnumerable<TResult>> BulkInsertAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default);
        #endregion
    }
    #endregion

    #region IBulkInserter
    /// <summary>
    /// Bulk Inserter Interface
    /// </summary>
    public interface INoResultBulkInserter<TMessage>
    {
        #region Properties
        Func<IEnumerable<TMessage>, Task> InsertHandler { get; set; }

        Action<BulkInsertContextContext> OnInsertCallBack { get; set; }
        #endregion

        #region Bulk Insert Method
        Task BulkInserterAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default);

        // Task BulkInserterAsync<TMessage>(IEnumerable<TMessage> messages, Func<IEnumerable<TMessage>> insertAction, CancellationToken cancellationToken = default);
        #endregion
    }
    #endregion

    #region IBulkInserter
    /// <summary>
    /// Bulk Inserter Interface
    /// </summary>
    public interface IPipeBulkInserter<TMessage, TResult>
    {
        #region Properties
        Func<IEnumerable<TMessage>, Task<IEnumerable<TResult>>> InsertHandler { get; set; }

        Action<BulkInsertContextContext<TResult>> OnInsertCallBack { get; set; }
        #endregion

        #region Bulk Insert Method
        Task<TResult[]> BulkInsertAsync(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default);

        // Task BulkInserterAsync<TMessage>(IEnumerable<TMessage> messages, Func<IEnumerable<TMessage>> insertAction, CancellationToken cancellationToken = default);
        #endregion
    }
    #endregion
}