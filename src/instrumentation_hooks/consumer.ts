import type * as amqp from 'amqplib';
import { safeExecuteInTheMiddle } from '@opentelemetry/instrumentation';
import { Span, SpanKind, SpanStatusCode, Tracer, context, diag, propagation, trace } from '@opentelemetry/api';
import {
  AfterConsumeInfo,
  ArnavmqInstrumentationConfig,
  BeforeProcessHook,
  BeforeRpcReplyHook,
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
  return async function beforeProcessMessage(e) {
    const message = e.message as amqp.Message & { properties: { [MESSAGE_STORED_SPAN]: Span } };
    const msgProperties = message.properties;
    const { headers } = msgProperties;
    const parentContext = propagation.extract(context.active(), headers);

    const span = tracer.startSpan(
      `${e.queue} receive`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          ...this.connection[CONNECTION_ATTRIBUTES],
          'messaging.destination.name': e.message.fields.exchange || DEFAULT_EXCHANGE_NAME,
          'messaging.rabbitmq.destination.routing_key': e.queue,
          'messaging.message.id': msgProperties.messageId,
          // Note: According to the specification the operation should be called 'deliver' since it triggers a registered callback, but 'deliver' is confusing for receiving a message.
          'messaging.operation': 'receive',
          'messaging.message.conversation_id': msgProperties.correlationId,
          'messaging.message.body.size': message.content.byteLength,
        },
      },
      parentContext,
    );

    if (config.subscribeHook) {
      safeExecuteInTheMiddle(
        () => config.subscribeHook!(span, e),
        (err) => {
          if (err) {
            diag.error('arnavmq instrumentation: subscribeHook error', err);
          }
        },
        true,
      );
    }
    message.properties[MESSAGE_STORED_SPAN] = span;
  };
}

export async function afterProcessMessageCallback(e: AfterConsumeInfo) {
  const message = e.message as amqp.Message & { properties: { [MESSAGE_STORED_SPAN]: Span } };
  const span = message.properties[MESSAGE_STORED_SPAN];

  if (e.error) {
    span.recordException(e.error);
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: `consumed message processing failed: ${e.error.message}`,
    });
  }

  if (e.ackError) {
    span.recordException(e.ackError);
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: `consumed message failed ack after processing: ${e.ackError.message}`,
    });
  }

  if (e.rejectError) {
    span.recordException(e.rejectError);
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: `consumed message failed to reject after failing to process: ${e.rejectError.message}`,
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

export async function afterRpcReplyCallback(e: RpcResultInfo) {
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
