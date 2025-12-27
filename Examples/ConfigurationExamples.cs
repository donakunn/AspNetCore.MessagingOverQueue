using MessagingOverQueue.Configuration.Sources;
using MessagingOverQueue.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MessagingOverQueue.Examples;

/// <summary>
/// Examples demonstrating different configuration approaches for RabbitMQ messaging.
/// </summary>
public static class ConfigurationExamples
{
    /// <summary>
    /// Example 1: Pure fluent configuration (original approach).
    /// Best for: Simple scenarios, testing, when configuration is fully code-based.
    /// </summary>
    public static void FluentConfiguration(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest")
            .WithConnectionName("MyApplication")
            .WithChannelPoolSize(10)
            .AddExchange(ex => ex
                .WithName("orders-exchange")
                .AsTopic()
                .Durable())
            .AddQueue(q => q
                .WithName("orders-queue")
                .Durable()
                .WithDeadLetterExchange("dlx-exchange"))
            .AddBinding(b => b
                .FromExchange("orders-exchange")
                .ToQueue("orders-queue")
                .WithRoutingKey("orders.#")));
    }

    /// <summary>
    /// Example 2: Configuration from appsettings.json.
    /// Best for: Production environments, environment-specific configuration.
    /// </summary>
    public static void AppSettingsConfiguration(IServiceCollection services, IConfiguration configuration)
    {
        // Requires appsettings.json with RabbitMq section
        services.AddRabbitMqMessaging(configuration);
        
        // Or specify a custom section name
        services.AddRabbitMqMessaging(configuration, "Messaging:RabbitMq");
    }

    /// <summary>
    /// Example 3: Hybrid - AppSettings with fluent overrides.
    /// Best for: Production with environment-specific defaults + runtime overrides.
    /// </summary>
    public static void HybridConfiguration(IServiceCollection services, IConfiguration configuration)
    {
        services.AddRabbitMqMessaging(
            configuration,
            options => options
                // Override specific values from appsettings
                .WithConnectionName("MyApp-Override")
                // Add additional exchanges not in appsettings
                .AddExchange(ex => ex
                    .WithName("dynamic-exchange")
                    .AsTopic()
                    .Durable()));
    }

    /// <summary>
    /// Example 4: .NET Aspire integration.
    /// Best for: Microservices with Aspire orchestration, development environments.
    /// </summary>
    public static void AspireConfiguration(IServiceCollection services, IConfiguration configuration)
    {
        // Aspire automatically discovers RabbitMQ connection
        services.AddRabbitMqMessagingFromAspire(configuration);
        
        // Or with custom connection name
        services.AddRabbitMqMessagingFromAspire(configuration, "my-rabbitmq");
        
        // Or with fluent overrides
        services.AddRabbitMqMessagingFromAspire(
            configuration,
            options => options
                .AddQueue(q => q
                    .WithName("aspire-managed-queue")
                    .Durable()),
            "my-rabbitmq");
    }

    /// <summary>
    /// Example 5: Custom configuration sources with priority control.
    /// Best for: Advanced scenarios, secrets management, multi-source composition.
    /// </summary>
    public static void CustomSourceConfiguration(IServiceCollection services, IConfiguration configuration)
    {
        services.AddRabbitMqMessaging(composer =>
        {
            // Priority order: Aspire (25) < AppSettings (50) < Custom (75) < Fluent (100)
            
            // 1. Aspire provides defaults (priority: 25)
            composer.AddSource(new AspireConfigurationSource(configuration, "rabbitmq"));
            
            // 2. AppSettings for environment-specific config (priority: 50)
            composer.AddSource(new AppSettingsConfigurationSource(configuration));
            
            // 3. Custom source for secrets (priority: 75)
            composer.AddSource(new CustomSecretsConfigurationSource(priority: 75));
            
            // 4. Fluent API for final overrides (priority: 100)
            composer.AddSource(new FluentConfigurationSource(
                opts => opts.WithConnectionName("FinalOverride"),
                priority: 100));
        });
    }

    /// <summary>
    /// Example 6: Multiple appsettings sections.
    /// Best for: Multi-tenant scenarios, multiple RabbitMQ instances.
    /// </summary>
    public static void MultipleInstancesConfiguration(IServiceCollection services, IConfiguration configuration)
    {
        // First instance - default section
        services.AddRabbitMqMessaging(configuration);
        
        // Second instance - could register with different lifetime/options
        // Note: This requires advanced DI registration not shown here
    }

    /// <summary>
    /// Example 7: Development vs Production patterns.
    /// </summary>
    public static void EnvironmentSpecificConfiguration(
        IServiceCollection services, 
        IConfiguration configuration,
        string environment)
    {
        if (environment == "Development")
        {
            // Use Aspire for local development
            services.AddRabbitMqMessagingFromAspire(
                configuration,
                opts => opts
                    .WithConnectionName($"DevApp-{Environment.MachineName}"));
        }
        else
        {
            // Use appsettings + secrets for production
            services.AddRabbitMqMessaging(
                configuration,
                opts => opts
                    .WithConnectionName($"ProdApp-{environment}")
                    .UseSsl() // Force SSL in production
            );
        }
    }
}

/// <summary>
/// Example custom configuration source that could load from a secrets vault.
/// </summary>
public class CustomSecretsConfigurationSource : IRabbitMqConfigurationSource
{
    public int Priority { get; }

    public CustomSecretsConfigurationSource(int priority = 75)
    {
        Priority = priority;
    }

    public void Configure(Configuration.Options.RabbitMqOptions options)
    {
        // Example: Load credentials from a vault
        // In real implementation, this would be async and use actual vault service
        
        // Simulate loading from vault
        options.UserName = GetSecret("rabbitmq-username") ?? options.UserName;
        options.Password = GetSecret("rabbitmq-password") ?? options.Password;
        
        // Could also load connection strings, certificates, etc.
    }

    private string? GetSecret(string key)
    {
        // Placeholder for actual vault integration
        // Could use Azure Key Vault, HashiCorp Vault, AWS Secrets Manager, etc.
        return null;
    }
}
