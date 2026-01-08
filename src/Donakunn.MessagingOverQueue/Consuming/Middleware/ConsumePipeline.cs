namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Builds and executes the consume middleware pipeline.
/// </summary>
public class ConsumePipeline
{
    private readonly IEnumerable<IConsumeMiddleware> _middlewares;
    private readonly Func<ConsumeContext, CancellationToken, Task> _terminalHandler;

    public ConsumePipeline(IEnumerable<IConsumeMiddleware> middlewares, Func<ConsumeContext, CancellationToken, Task> terminalHandler)
    {
        _middlewares = middlewares;
        _terminalHandler = terminalHandler;
    }

    /// <summary>
    /// Executes the pipeline for the given context.
    /// </summary>
    public Task ExecuteAsync(ConsumeContext context, CancellationToken cancellationToken)
    {
        var pipeline = BuildPipeline();
        return pipeline(context, cancellationToken);
    }

    private Func<ConsumeContext, CancellationToken, Task> BuildPipeline()
    {
        Func<ConsumeContext, CancellationToken, Task> current = _terminalHandler;

        foreach (var middleware in _middlewares.Reverse())
        {
            var next = current;
            var currentMiddleware = middleware;
            current = (ctx, ct) => currentMiddleware.InvokeAsync(ctx, next, ct);
        }

        return current;
    }
}

