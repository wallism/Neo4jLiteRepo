using Neo4j.Driver;

namespace Neo4jLiteRepo.Helpers;

/// <summary>
/// Helper class for executing Neo4j operations within sessions and transactions,
/// reducing boilerplate code across repository methods.
/// </summary>
public static class Neo4jSessionHelper
{
    /// <summary>
    /// Executes a write operation within a new session and transaction, returning a result.
    /// Handles session creation, transaction management, commit/rollback, and disposal.
    /// </summary>
    /// <typeparam name="T">The return type of the operation.</typeparam>
    /// <param name="driver">The Neo4j driver instance.</param>
    /// <param name="work">The async function to execute within the transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result of the work function.</returns>
    public static async Task<T> ExecuteInWriteTransactionAsync<T>(
        IDriver driver,
        Func<IAsyncTransaction, Task<T>> work,
        CancellationToken ct = default)
    {
        await using var session = driver.AsyncSession();
        return await ExecuteInWriteTransactionAsync(session, work, ct);
    }

    /// <summary>
    /// Executes a write operation within an existing session and new transaction, returning a result.
    /// </summary>
    public static async Task<T> ExecuteInWriteTransactionAsync<T>(
        IAsyncSession session,
        Func<IAsyncTransaction, Task<T>> work,
        CancellationToken ct = default)
    {
        var tx = await session.BeginTransactionAsync();
        try
        {
            ct.ThrowIfCancellationRequested();
            var result = await work(tx);
            await tx.CommitAsync();
            return result;
        }
        catch
        {
            try { await tx.RollbackAsync(); }
            catch { /* Suppress rollback errors */ }
            throw;
        }
    }

    /// <summary>
    /// Executes a write operation within a new session and transaction, with no return value.
    /// </summary>
    public static async Task ExecuteInWriteTransactionAsync(
        IDriver driver,
        Func<IAsyncTransaction, Task> work,
        CancellationToken ct = default)
    {
        await using var session = driver.AsyncSession();
        await ExecuteInWriteTransactionAsync(session, work, ct);
    }

    /// <summary>
    /// Executes a write operation within an existing session and new transaction, with no return value.
    /// </summary>
    public static async Task ExecuteInWriteTransactionAsync(
        IAsyncSession session,
        Func<IAsyncTransaction, Task> work,
        CancellationToken ct = default)
    {
        var tx = await session.BeginTransactionAsync();
        try
        {
            ct.ThrowIfCancellationRequested();
            await work(tx);
            await tx.CommitAsync();
        }
        catch
        {
            try { await tx.RollbackAsync(); }
            catch { /* Suppress rollback errors */ }
            throw;
        }
    }

    /// <summary>
    /// Executes a read operation within a new session and transaction, returning a result.
    /// </summary>
    public static async Task<T> ExecuteInReadTransactionAsync<T>(
        IDriver driver,
        Func<IAsyncTransaction, Task<T>> work,
        CancellationToken ct = default)
    {
        await using var session = driver.AsyncSession();
        return await ExecuteInReadTransactionAsync(session, work, ct);
    }

    /// <summary>
    /// Executes a read operation within an existing session and new transaction, returning a result.
    /// </summary>
    public static async Task<T> ExecuteInReadTransactionAsync<T>(
        IAsyncSession session,
        Func<IAsyncTransaction, Task<T>> work,
        CancellationToken ct = default)
    {
        var tx = await session.BeginTransactionAsync();
        try
        {
            ct.ThrowIfCancellationRequested();
            var result = await work(tx);
            await tx.CommitAsync();
            return result;
        }
        catch
        {
            try { await tx.RollbackAsync(); }
            catch { /* Suppress rollback errors */ }
            throw;
        }
    }
}
