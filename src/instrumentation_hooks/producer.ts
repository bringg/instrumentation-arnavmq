import { safeExecuteInTheMiddle } from '@opentelemetry/instrumentation';
import { Span, SpanKind, SpanStatusCode, Tracer, context, diag, propagation, trace } from '@opentelemetry/api';
import { ArnavmqInstrumentationConfig, BeforePublishHook, PublishResultInfo } from '../types';
import {
  CONNECTION_ATTRIBUTES,
  DEFAULT_EXCHANGE_NAME,
  MESSAGE_PUBLISH_ATTEMPT_SPAN,
  MESSAGE_PUBLISH_ROOT_SPAN,
} from '../consts';

export function getBeforePublishHook(config: ArnavmqInstrumentationConfig, tracer: Tracer): BeforePublishHook {
  return async function beforePublish(e) {
    const msgProperties = e.properties as typeof e.properties & {
      [MESSAGE_PUBLISH_ATTEMPT_SPAN]: Span;
      [MESSAGE_PUBLISH_ROOT_SPAN]: Span;
    };

    let exchange = DEFAULT_EXCHANGE_NAME;
    let { queue } = e;
    if (msgProperties.routingKey) {
      exchange = e.queue;
      queue = msgProperties.routingKey;
    }

    let parentSpan = msgProperties[MESSAGE_PUBLISH_ROOT_SPAN];
    if (!parentSpan) {
      // In case the underlying connection wasn't initialized yet, initialize it (could happen if this is the first time it is accessed)
      if (!this.connection[CONNECTION_ATTRIBUTES]) {
        await this.connection.getConnection();
      }

      // The root span.
      parentSpan = tracer.startSpan(`${exchange} -> ${queue} create${msgProperties.rpc ? ' rpc' : ''}`, {
        kind: SpanKind.CLIENT,
        attributes: {
          ...this.connection[CONNECTION_ATTRIBUTES],
          'messaging.destination.name': exchange,
          'messaging.rabbitmq.destination.routing_key': queue,
          'messaging.rabbitmq.message.rpc': !!msgProperties.rpc,
          'messaging.message.conversation_id': msgProperties.correlationId,
          'messaging.message.id': msgProperties.messageId,
          // The entire publish operation is 'create', with each underlying actual message send and potential retries is a separate child 'publish'.
          'messaging.operation': 'create',
          'messaging.message.body.size': e.parsedMessage.byteLength,
        },
      });
      msgProperties[MESSAGE_PUBLISH_ROOT_SPAN] = parentSpan;
    }

    const parentContext = trace.setSpan(context.active(), parentSpan);

    const span = tracer.startSpan(
      `${exchange} -> ${queue} publish (attempt ${e.currentRetry})`,
      {
        kind: SpanKind.PRODUCER,
        attributes: {
          'messaging.rabbitmq.message.retry_number': e.currentRetry,
          'messaging.operation': 'publish',
        },
      },
      parentContext,
    );
    msgProperties.headers = msgProperties.headers || {};

    propagation.inject(trace.setSpan(parentContext, span), msgProperties.headers);

    if (config.publishHook) {
      safeExecuteInTheMiddle(
        () => config.publishHook!(parentSpan, e),
        (err) => {
          if (err) {
            diag.error('arnavmq instrumentation: publishHook error', err);
          }
        },
        true,
      );
    }

    msgProperties[MESSAGE_PUBLISH_ATTEMPT_SPAN] = span;
  };
}

export async function afterPublishCallback(e: PublishResultInfo) {
  const msgProperties = e.properties as typeof e.properties & {
    [MESSAGE_PUBLISH_ATTEMPT_SPAN]: Span;
    [MESSAGE_PUBLISH_ROOT_SPAN]: Span;
  };
  const rootSpan = msgProperties[MESSAGE_PUBLISH_ROOT_SPAN];
  const attemptSpan = msgProperties[MESSAGE_PUBLISH_ATTEMPT_SPAN];

  if (e.error) {
    attemptSpan.recordException(e.error);
    attemptSpan.setStatus({
      code: SpanStatusCode.ERROR,
      message: `send attempt failed on retry ${e.currentRetry}: ${e.error.message}`,
    });

    if (e.shouldRetry) {
      attemptSpan.end();
      // Root span will continue on subsequent retries.
      return;
    }
    rootSpan.setStatus({ code: SpanStatusCode.ERROR, message: `send failed after ${e.currentRetry} attempts` });
  }
  attemptSpan.end();
  rootSpan.end();
}
