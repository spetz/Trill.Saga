using System.Linq;
using System.Text;
using Chronicle;
using Convey;
using Convey.Auth;
using Convey.CQRS.Commands;
using Convey.CQRS.Events;
using Convey.CQRS.Queries;
using Convey.HTTP;
using Convey.MessageBrokers;
using Convey.MessageBrokers.CQRS;
using Convey.MessageBrokers.Outbox;
using Convey.MessageBrokers.Outbox.Mongo;
using Convey.MessageBrokers.RabbitMQ;
using Convey.Metrics.Prometheus;
using Convey.Persistence.MongoDB;
using Convey.Persistence.Redis;
using Convey.Tracing.Jaeger;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Trill.Saga.Clients;
using Trill.Saga.Decorators;
using Trill.Saga.Events.External;
using Trill.Saga.Logging;
using Trill.Saga.Services;

namespace Trill.Saga
{
    public static class Extensions
    {
        public static IConveyBuilder AddCore(this IConveyBuilder builder)
        {
            builder.Services
                .AddScoped<LogContextMiddleware>()
                .AddChronicle()
                .AddScoped<IAdApiClient, AdApiHttpClient>()
                .AddScoped<IMessageBroker, MessageBroker>();

            builder
                .AddCommandHandlers()
                .AddEventHandlers()
                .AddInMemoryCommandDispatcher()
                .AddInMemoryEventDispatcher()
                .AddQueryHandlers()
                .AddInMemoryQueryDispatcher()
                .AddHttpClient()
                .AddRabbitMq()
                .AddMessageOutbox(o => o.AddMongo())
                .AddMongo()
                .AddRedis()
                .AddPrometheus()
                .AddJaeger()
                .AddJwt();
            
            builder.Services.TryDecorate(typeof(ICommandHandler<>), typeof(LoggingCommandHandlerDecorator<>));
            builder.Services.TryDecorate(typeof(IEventHandler<>), typeof(LoggingEventHandlerDecorator<>));
            builder.Services.TryDecorate(typeof(ICommandHandler<>), typeof(OutboxCommandHandlerDecorator<>));
            builder.Services.TryDecorate(typeof(IEventHandler<>), typeof(OutboxEventHandlerDecorator<>));

            return builder;
        }

        public static IApplicationBuilder UseCore(this IApplicationBuilder app)
        {
            app.UseConvey()
                .UseAccessTokenValidator()
                .UseAuthentication()
                .UseRabbitMq()
                .SubscribeEvent<AdApproved>()
                .SubscribeEvent<AdPaid>()
                .SubscribeEvent<AdPublished>()
                .SubscribeEvent<AdActionRejected>()
                .SubscribeEvent<StoryActionRejected>();

            return app;
        }
        
        internal static CorrelationContext GetCorrelationContext(this IHttpContextAccessor accessor)
            => accessor.HttpContext?.Request.Headers.TryGetValue("Correlation-Context", out var json) is true
                ? JsonConvert.DeserializeObject<CorrelationContext>(json.FirstOrDefault())
                : null;
        
        internal static string GetSpanContext(this IMessageProperties messageProperties, string header)
        {
            if (messageProperties is null)
            {
                return string.Empty;
            }

            if (messageProperties.Headers.TryGetValue(header, out var span) && span is byte[] spanBytes)
            {
                return Encoding.UTF8.GetString(spanBytes);
            }

            return string.Empty;
        }
    }
}