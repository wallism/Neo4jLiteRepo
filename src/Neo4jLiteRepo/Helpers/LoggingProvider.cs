using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Neo4jLiteRepo.Helpers
{
    /// <summary>
    /// Provides logging services to static classes in a framework-agnostic way.
    /// </summary>
    public static class LoggingProvider
    {
        private static ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;

        /// <summary>
        /// Configures the logger factory to be used by static classes.
        /// </summary>
        /// <param name="loggerFactory">The logger factory to use.</param>
        public static void Configure(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        }

        /// <summary>
        /// Creates a logger for the specified type.
        /// </summary>
        /// <typeparam name="T">The type to create the logger for.</typeparam>
        /// <returns>A logger instance.</returns>
        public static ILogger CreateLogger<T>()
        {
            return _loggerFactory.CreateLogger<T>();
        }

        /// <summary>
        /// Creates a logger with the specified category name.
        /// </summary>
        /// <param name="categoryName">The category name for the logger.</param>
        /// <returns>A logger instance.</returns>
        public static ILogger CreateLogger(string categoryName)
        {
            return _loggerFactory.CreateLogger(categoryName);
        }
        
        /// <summary>
        /// Adds the logging provider configuration to the service collection.
        /// </summary>
        /// <param name="services">The service collection to add the configuration to.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddLoggingProviderConfiguration(this IServiceCollection services)
        {
            // Ensure this runs after the ILoggerFactory is fully configured
            services.AddSingleton<ILoggingProviderConfigurator, LoggingProviderConfigurator>();
            return services;
        }
        
        /// <summary>
        /// Internal class to handle configuration through DI
        /// </summary>
        private class LoggingProviderConfigurator : ILoggingProviderConfigurator
        {
            public LoggingProviderConfigurator(ILoggerFactory loggerFactory)
            {
                Configure(loggerFactory);
            }
        }
        
        /// <summary>
        /// Interface for the logging provider configurator to work with DI
        /// </summary>
        private interface ILoggingProviderConfigurator { }
    }

}
