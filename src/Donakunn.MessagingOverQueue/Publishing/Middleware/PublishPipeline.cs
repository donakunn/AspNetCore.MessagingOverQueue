namespace MessagingOverQueue.src.Publishing.Middleware;

/// <summary>
/// Builds and executes the publish middleware pipeline.
/// </summary>
public class PublishPipeline
{
    private readonly IEnumerable<IPublishMiddleware> _middlewares;
    private readonly Func<PublishContext, CancellationToken, Task> _terminalHandler;

    public PublishPipeline(IEnumerable<IPublishMiddleware> middlewares, Func<PublishContext, CancellationToken, Task> terminalHandler)
    {
        _middlewares = middlewares;
        _terminalHandler = terminalHandler;
    }

    /// <summary>
    /// Executes the pipeline for the given context.
    /// </summary>
    public Task ExecuteAsync(PublishContext context, CancellationToken cancellationToken)
    {
        var pipeline = BuildPipeline();
        return pipeline(context, cancellationToken);
    }

    private Func<PublishContext, CancellationToken, Task> BuildPipeline()
    {
        Func<PublishContext, CancellationToken, Task> current = _terminalHandler;

        foreach (var middleware in _middlewares.Reverse())
        {
            var next = current;
            var currentMiddleware = middleware;
            current = (ctx, ct) => currentMiddleware.InvokeAsync(ctx, next, ct);
        }

        return current;
    }
}

