namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Builds and executes the consume middleware pipeline.
/// Middlewares implementing <see cref="IOrderedConsumeMiddleware"/> are sorted by their Order property.
/// </summary>
public class ConsumePipeline
{
    private readonly Func<ConsumeContext, CancellationToken, Task> _pipeline;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConsumePipeline"/> class.
    /// The pipeline is built once at construction time for thread safety.
    /// </summary>
    /// <param name="middlewares">The middlewares to include in the pipeline.</param>
    /// <param name="terminalHandler">The terminal handler that processes the message.</param>
    public ConsumePipeline(IEnumerable<IConsumeMiddleware> middlewares, Func<ConsumeContext, CancellationToken, Task> terminalHandler)
    {
        _pipeline = BuildPipeline(middlewares, terminalHandler);
    }

    /// <summary>
    /// Executes the pipeline for the given context.
    /// </summary>
    public Task ExecuteAsync(ConsumeContext context, CancellationToken cancellationToken)
    {
        return _pipeline(context, cancellationToken);
    }

    /// <summary>
    /// Builds the middleware pipeline, sorting ordered middlewares by their Order property.
    /// </summary>
    private static Func<ConsumeContext, CancellationToken, Task> BuildPipeline(
        IEnumerable<IConsumeMiddleware> middlewares,
        Func<ConsumeContext, CancellationToken, Task> terminalHandler)
    {
        // Sort middlewares by order - ordered middlewares first (by Order), then unordered (by Default)
        var sortedMiddlewares = middlewares
            .Select(m => new
            {
                Middleware = m,
                Order = m is IOrderedConsumeMiddleware ordered ? ordered.Order : MiddlewareOrder.Default
            })
            .OrderBy(x => x.Order)
            .Select(x => x.Middleware)
            .ToList();

        Func<ConsumeContext, CancellationToken, Task> current = terminalHandler;

        // Build pipeline in reverse order (last middleware wraps the terminal handler)
        foreach (var middleware in Enumerable.Reverse(sortedMiddlewares))
        {
            var next = current;
            var currentMiddleware = middleware;
            current = (ctx, ct) => currentMiddleware.InvokeAsync(ctx, next, ct);
        }

        return current;
    }
}

