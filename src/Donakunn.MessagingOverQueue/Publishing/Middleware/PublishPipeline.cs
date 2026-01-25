namespace Donakunn.MessagingOverQueue.Publishing.Middleware;

/// <summary>
/// Builds and executes the publish middleware pipeline.
/// </summary>
public class PublishPipeline
{
    private readonly Func<PublishContext, CancellationToken, Task> _pipeline;

    public PublishPipeline(IEnumerable<IPublishMiddleware> middlewares, Func<PublishContext, CancellationToken, Task> terminalHandler)
    {
        _pipeline = BuildPipeline(middlewares, terminalHandler);
    }

    /// <summary>
    /// Executes the pipeline for the given context.
    /// </summary>
    public Task ExecuteAsync(PublishContext context, CancellationToken cancellationToken)
    {
        return _pipeline(context, cancellationToken);
    }

    private static Func<PublishContext, CancellationToken, Task> BuildPipeline(
        IEnumerable<IPublishMiddleware> middlewares,
        Func<PublishContext, CancellationToken, Task> terminalHandler)
    {
        Func<PublishContext, CancellationToken, Task> current = terminalHandler;

        foreach (var middleware in middlewares.Reverse())
        {
            var next = current;
            var currentMiddleware = middleware;
            current = (ctx, ct) => currentMiddleware.InvokeAsync(ctx, next, ct);
        }

        return current;
    }
}

