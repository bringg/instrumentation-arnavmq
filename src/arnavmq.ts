import {
  InstrumentationBase,
  InstrumentationModuleDefinition,
  InstrumentationNodeModuleDefinition,
  safeExecuteInTheMiddle,
} from '@opentelemetry/instrumentation';
import { Span, SpanKind, SpanStatusCode, context, diag, propagation, trace } from '@opentelemetry/api';

import type * as amqp from 'amqplib';
import {
  CONNECTION_ATTRIBUTES,
  DEFAULT_EXCHANGE_NAME,
  MESSAGE_STORED_SPAN,
  MESSAGE_RPC_REPLY_STORED_SPAN,
  RPC_REPLY_DESTINATION_NAME,
  getConnectionConfigAttributes,
  getServerPropertiesAttributes,
  MESSAGE_PUBLISH_ROOT_SPAN,
  MESSAGE_PUBLISH_ATTEMPT_SPAN,
  INSTRUMENTATION_ARNAVMQ_VERSION,
} from './utils';
import {
  AfterConnectInfo,
  AfterConsumeInfo,
  ArnavmqInstrumentationConfig,
  ArnavmqModule,
  BeforeProcessHook,
  BeforePublishHook,
  BeforeRpcReplyHook,
  ConnectionConfig,
  InstrumentedConnection,
  PublishResultInfo,
  RpcResultInfo,
} from './types';

async function afterConnectCallback(this: InstrumentedConnection, e: AfterConnectInfo) {
  if (e.error) {
    return;
  }

  const optionsAttributes = getConnectionConfigAttributes(e.config);
  const serverPropertiesAttributes = getServerPropertiesAttributes(e.connection.connection);
  this[CONNECTION_ATTRIBUTES] = { ...optionsAttributes, ...serverPropertiesAttributes };
}

async function afterProcessMessageCallback(e: AfterConsumeInfo) {
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

async function afterPublishCallback(e: PublishResultInfo) {
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

async function afterRpcReplyCallback(e: RpcResultInfo) {
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

export default class ArnavmqInstrumentation extends InstrumentationBase {
  protected override _config!: ArnavmqInstrumentationConfig;

  private _patchedModule: boolean;

  private _beforeProcessMessageCallback: BeforeProcessHook;

  private _beforeRpcReplyCallback: BeforeRpcReplyHook;

  private _beforePublishCallback: BeforePublishHook;

  constructor(config?: ArnavmqInstrumentationConfig) {
    super('instrumentation-arnavmq', INSTRUMENTATION_ARNAVMQ_VERSION, config);
    this._patchedModule = false;
    this._beforeProcessMessageCallback = this.getBeforeProcessMessageHook();
    this._beforeRpcReplyCallback = this.getBeforeRpcReplyHook();
    this._beforePublishCallback = this.getBeforePublishHook();
  }

  protected override init(): void | InstrumentationModuleDefinition<any> | InstrumentationModuleDefinition<any>[] {
    // No unpatching, since we wrap the entire module export and can't unwrap it.
    return new InstrumentationNodeModuleDefinition('arnavmq', ['>=0.16.0'], this.patchArnavmq.bind(this));
  }

  patchArnavmq(moduleExports: ArnavmqModule) {
    if (this._patchedModule) {
      return moduleExports;
    }

    const arnavmqFactory: ArnavmqModule = (config: ConnectionConfig) => {
      const arnavmq = moduleExports(config);
      const { hooks } = arnavmq;
      hooks.connection.afterConnect(afterConnectCallback);
      hooks.consumer.beforeProcessMessage(this._beforeProcessMessageCallback);
      hooks.consumer.afterProcessMessage(afterProcessMessageCallback);
      hooks.consumer.beforeRpcReply(this._beforeRpcReplyCallback);
      hooks.consumer.afterRpcReply(afterRpcReplyCallback);
      hooks.producer.beforePublish(this._beforePublishCallback);
      hooks.producer.afterPublish(afterPublishCallback);

      return arnavmq;
    };

    this._patchedModule = true;
    return arnavmqFactory;
  }

  getBeforeProcessMessageHook(): BeforeProcessHook {
    const self = this;
    const config = this._config;

    return async function beforeProcessMessage(e) {
      const message = e.message as amqp.Message & { properties: { [MESSAGE_STORED_SPAN]: Span } };
      const msgProperties = message.properties;
      const { headers } = msgProperties;
      const parentContext = propagation.extract(context.active(), headers);

      const span = self.tracer.startSpan(
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

  getBeforeRpcReplyHook(): BeforeRpcReplyHook {
    const self = this;
    const config = this._config;

    return async function beforeRpcReply(this: { connection: InstrumentedConnection }, e) {
      const receiveProperties = e.receiveProperties as amqp.MessageProperties & {
        [MESSAGE_STORED_SPAN]: Span;
        [MESSAGE_RPC_REPLY_STORED_SPAN]: Span;
      };

      const consumer = this;

      const parentSpan = receiveProperties[MESSAGE_STORED_SPAN];
      const parentContext = trace.setSpan(context.active(), parentSpan);
      const span = self.tracer.startSpan(
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

  getBeforePublishHook(): BeforePublishHook {
    const self = this;
    const config = this._config;

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
        parentSpan = self.tracer.startSpan(`${exchange} -> ${queue} create${msgProperties.rpc ? ' rpc' : ''}`, {
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

      const span = self.tracer.startSpan(
        `${exchange} -> ${queue} create (attempt ${e.currentRetry})`,
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
}
