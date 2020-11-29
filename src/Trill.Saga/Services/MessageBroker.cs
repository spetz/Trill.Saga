using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Convey;
using Convey.CQRS.Commands;
using Convey.HTTP;
using Convey.MessageBrokers;
using Convey.MessageBrokers.Outbox;
using Convey.MessageBrokers.RabbitMQ;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Trill.Saga.Services
{
    internal class MessageBroker : IMessageBroker
    {
        private const string DefaultSpanContextHeader = "span_context";
        private readonly IBusPublisher _busPublisher;
        private readonly IMessageOutbox _outbox;
        private readonly ICorrelationContextAccessor _contextAccessor;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly IMessagePropertiesAccessor _messagePropertiesAccessor;
        private readonly ICorrelationIdFactory _correlationIdFactory;
        private readonly ILogger<IMessageBroker> _logger;
        private readonly string _spanContextHeader;

        public MessageBroker(IBusPublisher busPublisher, IMessageOutbox outbox,
            ICorrelationContextAccessor contextAccessor, IHttpContextAccessor httpContextAccessor,
            IMessagePropertiesAccessor messagePropertiesAccessor, ICorrelationIdFactory correlationIdFactory,
            RabbitMqOptions options, ILogger<IMessageBroker> logger)
        {
            _busPublisher = busPublisher;
            _outbox = outbox;
            _contextAccessor = contextAccessor;
            _httpContextAccessor = httpContextAccessor;
            _messagePropertiesAccessor = messagePropertiesAccessor;
            _correlationIdFactory = correlationIdFactory;
            _logger = logger;
            _spanContextHeader = string.IsNullOrWhiteSpace(options.SpanContextHeader)
                ? DefaultSpanContextHeader
                : options.SpanContextHeader;
        }

        public Task SendAsync(params ICommand[] commands) => SendAsync(commands?.AsEnumerable());

        private async Task SendAsync(IEnumerable<ICommand> commands)
        {
            if (commands is null)
            {
                return;
            }

            var messageProperties = _messagePropertiesAccessor.MessageProperties;
            var originatedMessageId = messageProperties?.MessageId;
            var correlationId = _correlationIdFactory.Create();
            var spanContext = messageProperties?.GetSpanContext(_spanContextHeader);
            var correlationContext = _contextAccessor.CorrelationContext ??
                                     _httpContextAccessor.GetCorrelationContext();
            var headers = new Dictionary<string, object>();

            foreach (var command in commands)
            {
                if (command is null)
                {
                    continue;
                }

                var messageId = Guid.NewGuid().ToString("N");
                _logger.LogTrace($"Sending command: {command.GetType().Name.Underscore()} [ID: '{messageId}'].");
                if (_outbox.Enabled)
                {
                    await _outbox.SendAsync(command, originatedMessageId, messageId, correlationId, spanContext,
                        correlationContext, headers);
                    continue;
                }

                await _busPublisher.PublishAsync(command, messageId, correlationId, spanContext, correlationContext,
                    headers);
            }
        }
    }
}