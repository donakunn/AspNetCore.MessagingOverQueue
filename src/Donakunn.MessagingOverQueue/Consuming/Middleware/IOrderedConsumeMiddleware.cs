namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Interface for consume middleware that specifies its execution order.
/// Middleware implementing this interface will be sorted by <see cref="Order"/> before execution.
/// Lower values execute first (outer middleware), higher values execute later (inner middleware).
/// </summary>
public interface IOrderedConsumeMiddleware : IConsumeMiddleware
{
    /// <summary>
    /// The execution order of this middleware.
    /// Use constants from <see cref="MiddlewareOrder"/> for standard ordering.
    /// </summary>
    int Order { get; }
}
