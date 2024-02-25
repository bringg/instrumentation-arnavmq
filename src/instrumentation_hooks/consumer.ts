import type * as amqp from 'amqplib';
import { safeExecuteInTheMiddle } from '@opentelemetry/instrumentation';
import { Span, SpanKind, SpanStatusCode, Tracer, context, diag, propagation, trace } from '@opentelemetry/api';
import {
  AfterConsumeInfo,
  ArnavmqInstrumentationConfig,
  BeforeProcessHook,
  BeforeRpcReplyHook,
  ConsumeInfo,
  InstrumentedConnection,
  RpcResultInfo,
} from '../types';
import {
  CONNECTION_ATTRIBUTES,
  DEFAULT_EXCHANGE_NAME,
  MESSAGE_RPC_REPLY_STORED_SPAN,
  MESSAGE_STORED_SPAN,
  RPC_REPLY_DESTINATION_NAME,
} from '../consts';

export function getBeforeProcessMessageHook(config: ArnavmqInstrumentationConfig, tracer: Tracer): BeforeProcessHook {
  return async function beforeProcessMessage(event: ConsumeInfo): Promise<void> {
    const message = event.action.message as amqp.Message & { properties: { [MESSAGE_STORED_SPAN]: Span } };
    const msgProperties = message.properties;
    const { headers } = msgProperties;
    const parentContext = propagation.extract(context.active(), headers);

    const span = tracer.startSpan(
      `${event.queue} receive`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          ...this.connection[CONNECTION_ATTRIBUTES],
          'messaging.destination.name': event.action.message.fields.exchange || DEFAULT_EXCHANGE_NAME,
          'messaging.rabbitmq.destination.routing_key': event.queue,
          'messaging.message.id': msgProperties.messageId,
          // NOTE: According to the specification the operation should be called 'deliver' since it triggers a registered callback, but 'deliver' is confusing for receiving a message.
          'messaging.operation': 'receive',
          'messaging.message.conversation_id': msgProperties.correlationId,
          'messaging.message.body.size': message.content.byteLength,
          'messaging.rabbitmq.message.rpc': !!msgProperties.replyTo,
        },
      },
      parentContext,
    );

    if (config.subscribeHook) {
      safeExecuteInTheMiddle(
        () => config.subscribeHook!(span, event),
        (err) => {
          if (err) {
            diag.error('arnavmq instrumentation: subscribeHook error', err);
          }
        },
        true,
      );
    }
    message.properties[MESSAGE_STORED_SPAN] = span;
    // We need to specifically assign it on the function parameter callback to add it to wrap it
    // eslint-disable-next-line no-param-reassign
    event.action.callback = context.bind(trace.setSpan(parentContext, span), event.action.callback);
  };
}

export async function afterProcessMessageHook(e: AfterConsumeInfo) {
  const message = e.message as amqp.Message & { properties: { [MESSAGE_STORED_SPAN]: Span } };
  const span = message.properties[MESSAGE_STORED_SPAN];

  if (e.error) {
    span.recordException(e.error);
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: `consumed message processing failed: ${e.error.message}`,
    });

    // We only `reject` on consume callback error, so there could only be a `rejectError` if there is an `error`
    if (e.rejectError) {
      span.recordException(e.rejectError);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: `consumed message failed to reject after failing to process: ${e.rejectError.message}`,
      });
    }
  } else if (e.ackError) {
    // We only `ack` if there was no consumer callback error, so there can only be `ackError` if there is no `error`.
    span.recordException(e.ackError);
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: `consumed message failed ack after processing: ${e.ackError.message}`,
    });
  }

  span.end();
}

export function getBeforeRpcReplyHook(config: ArnavmqInstrumentationConfig, tracer: Tracer): BeforeRpcReplyHook {
  return async function beforeRpcReply(this: { connection: InstrumentedConnection }, e) {
    const receiveProperties = e.receiveProperties as amqp.MessageProperties & {
      [MESSAGE_STORED_SPAN]: Span;
      [MESSAGE_RPC_REPLY_STORED_SPAN]: Span;
    };

    const consumer = this;

    const parentSpan = receiveProperties[MESSAGE_STORED_SPAN];
    const parentContext = trace.setSpan(context.active(), parentSpan);
    const span = tracer.startSpan(
      `${e.queue} -> ${RPC_REPLY_DESTINATION_NAME} publish`,
      {
        kind: SpanKind.PRODUCER,
        attributes: {
          ...consumer.connection[CONNECTION_ATTRIBUTES],
          'messaging.rabbitmq.destination.routing_key': receiveProperties.replyTo,
          'messaging.destination.temporary': true,
          'messaging.destination.name': DEFAULT_EXCHANGE_NAME,
          'messaging.message.conversation_id': receiveProperties.correlationId,
          'messaging.operation': 'publish',
          'messaging.message.body.size': e.serializedReply.byteLength,
          'messaging.rabbitmq.message.rpc': true,
        },
      },
      parentContext,
    );

    e.replyProperties.headers = e.replyProperties.headers || {};
    propagation.inject(parentContext, e.replyProperties.headers);

    if (config.rpcResponseHook) {
      safeExecuteInTheMiddle(
        () => config.rpcResponseHook!(span, e),
        (err) => {
          if (err) {
            diag.error('arnavmq instrumentation: subscribeHook error', err);
          }
        },
        true,
      );
    }
    receiveProperties[MESSAGE_RPC_REPLY_STORED_SPAN] = span;
  };
}

export async function afterRpcReplyHook(e: RpcResultInfo) {
  const receiveProperties = e.receiveProperties as amqp.MessageProperties & {
    [MESSAGE_RPC_REPLY_STORED_SPAN]: Span;
  };
  const span = receiveProperties[MESSAGE_RPC_REPLY_STORED_SPAN];

  if (e.error) {
    span.recordException(e.error);
    span.setStatus({ code: SpanStatusCode.ERROR, message: `rpc reply failed: ${e.error.message}` });
  }
  span.end();
}
