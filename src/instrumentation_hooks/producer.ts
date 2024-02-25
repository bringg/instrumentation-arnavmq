import { safeExecuteInTheMiddle } from '@opentelemetry/instrumentation';
import { Span, SpanKind, SpanStatusCode, Tracer, context, diag, propagation, trace } from '@opentelemetry/api';
import {
  ArnavmqInstrumentationConfig,
  BeforeProduceHook,
  InstrumentedConnection,
  ProduceInfo,
  ProduceResultInfo,
} from '../types';
import { CONNECTION_ATTRIBUTES, DEFAULT_EXCHANGE_NAME, MESSAGE_PUBLISH_SPAN } from '../consts';

async function getPublishSpan(
  tracer: Tracer,
  producer: {
    connection: InstrumentedConnection;
  },
  e: ProduceInfo,
): Promise<Span> {
  const msgProperties = e.properties as typeof e.properties & {
    [MESSAGE_PUBLISH_SPAN]: Span;
  };

  let exchange = DEFAULT_EXCHANGE_NAME;
  let { queue } = e;
  if (msgProperties.routingKey) {
    exchange = e.queue;
    queue = msgProperties.routingKey;
  }

  let publishSpan = msgProperties[MESSAGE_PUBLISH_SPAN];
  if (!publishSpan) {
    // The root span.
    publishSpan = tracer.startSpan(`${exchange} -> ${queue} publish${msgProperties.rpc ? ' rpc' : ''}`, {
      kind: SpanKind.CLIENT,
      attributes: {
        ...producer.connection[CONNECTION_ATTRIBUTES],
        'messaging.destination.name': exchange,
        'messaging.rabbitmq.destination.routing_key': queue,
        'messaging.rabbitmq.message.rpc': !!msgProperties.rpc,
        'messaging.message.conversation_id': msgProperties.correlationId,
        'messaging.message.id': msgProperties.messageId,
        'messaging.operation': 'publish',
        'messaging.message.body.size': e.parsedMessage.byteLength,
      },
    });
    msgProperties[MESSAGE_PUBLISH_SPAN] = publishSpan;
  }

  if (e.currentRetry > 0) {
    publishSpan.setAttribute('messaging.rabbitmq.message.reconnect_retry_number', e.currentRetry);
    publishSpan.addEvent('producer - publish connection retry starts', {
      'messaging.rabbitmq.message.reconnect_retry_number': e.currentRetry,
    });
  }

  return publishSpan;
}

export function getBeforeProduceHook(config: ArnavmqInstrumentationConfig, tracer: Tracer): BeforeProduceHook {
  return async function beforeProduce(event: ProduceInfo): Promise<void> {
    const publishSpan = await getPublishSpan(tracer, this, event);
    // We need to specifically assign it on the function parameter headers to add it to the request
    // eslint-disable-next-line no-param-reassign
    event.properties.headers = event.properties.headers || {};
    propagation.inject(trace.setSpan(context.active(), publishSpan), event.properties.headers);

    if (config.produceHook) {
      safeExecuteInTheMiddle(
        () => config.produceHook!(publishSpan, event),
        (err) => {
          if (err) {
            diag.error('arnavmq instrumentation: produceHook error', err);
          }
        },
        true,
      );
    }
  };
}

export async function afterPublishCallback(e: ProduceResultInfo) {
  const msgProperties = e.properties as typeof e.properties & {
    [MESSAGE_PUBLISH_SPAN]: Span;
  };
  const publishSpan = msgProperties[MESSAGE_PUBLISH_SPAN];

  if (e.error) {
    publishSpan.recordException(e.error);

    if (e.shouldRetry) {
      // Root span will continue on subsequent retries.
      return;
    }
    publishSpan.setStatus({
      code: SpanStatusCode.ERROR,
      message: `send failed after ${e.currentRetry} connection retry attempts`,
    });
  }
  publishSpan.end();
}
