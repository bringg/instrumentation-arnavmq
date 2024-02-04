import {
  InstrumentationBase,
  InstrumentationModuleDefinition,
  InstrumentationNodeModuleDefinition,
  safeExecuteInTheMiddle,
} from '@opentelemetry/instrumentation';
import { Span, SpanKind, SpanStatusCode, context, diag, propagation, trace } from '@opentelemetry/api';

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
} from './utils';
import {
  AfterConnectHook,
  AfterPublishHook,
  AfterRpcHook,
  ArnavmqInstrumentationConfig,
  ArnavmqModule,
  ConnectionConfig,
  InstrumentedConnection,
  ProduceSettings,
} from './types';
import type * as amqp from 'amqplib';

export class ArnavmqInstrumentation extends InstrumentationBase {
  protected override _config!: ArnavmqInstrumentationConfig;

  private _patchedModule: boolean;

  private _afterConnectCallback: AfterConnectHook;
  private _beforeProcessMessageCallback: (
    this: { connection: InstrumentedConnection },
    e: {
      queue: string;
      message: amqp.Message;
      content: unknown;
    },
  ) => Promise<void>;
  private _afterProcessMessageCallback: (e: {
    message: amqp.Message;
    content: unknown;
    error?: Error;
    rejectError?: Error;
    ackError?: Error;
  }) => Promise<void>;
  private _beforeRpcReplyCallback: (e: {
    receiveProperties: amqp.MessageProperties;
    replyProperties: amqp.MessageProperties;
    queue: string;
    reply: unknown;
    serializedReply: Buffer;
  }) => Promise<void>;
  private _afterRpcReplyCallback: AfterRpcHook;
  private _beforePublishCallback: (
    this: { connection: InstrumentedConnection },
    e: {
      queue: string;
      message: unknown;
      parsedMessage: Buffer;
      properties: ProduceSettings;
      currentRetry: number;
    },
  ) => Promise<void>;
  private _afterPublishCallback: AfterPublishHook;

  constructor(config?: ArnavmqInstrumentationConfig) {
    // TODO: Generate a version file on ci to read the version from.
    super('instrumentation-arnavmq', '0.0.1', config);
    this._patchedModule = false;
    this._afterConnectCallback = this.getAfterConnectionHook();
    this._beforeProcessMessageCallback = this.getBeforeProcessMessageHook();
    this._afterProcessMessageCallback = this.getAfterProcessMessageHook();
    this._beforeRpcReplyCallback = this.getBeforeRpcReplyHook();
    this._afterRpcReplyCallback = this.getAfterRpcReplyHook();
    this._beforePublishCallback = this.getBeforePublishHook();
    this._afterPublishCallback = this.getAfterPublishHook();
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
      const hooks = arnavmq.hooks;
      hooks.connection.afterConnect(this._afterConnectCallback);
      hooks.consumer.beforeProcessMessage(this._beforeProcessMessageCallback);
      hooks.consumer.afterProcessMessage(this._afterProcessMessageCallback);
      hooks.consumer.beforeRpcReply(this._beforeRpcReplyCallback);
      hooks.consumer.afterRpcReply(this._afterRpcReplyCallback);
      hooks.producer.beforePublish(this._beforePublishCallback);
      hooks.producer.afterPublish(this._afterPublishCallback);

      return arnavmq;
    };

    this._patchedModule = true;
    return arnavmqFactory;
  }

  getAfterConnectionHook(): AfterConnectHook {
    return async function afterConnect(e) {
      if (e.error) {
        return;
      }

      const optionsAttributes = getConnectionConfigAttributes(e.config);
      const serverPropertiesAttributes = getServerPropertiesAttributes(e.connection.connection);
      this[CONNECTION_ATTRIBUTES] = { ...optionsAttributes, ...serverPropertiesAttributes };
    };
  }

  getBeforeProcessMessageHook(): (
    this: { connection: InstrumentedConnection },
    e: { queue: string; message: amqp.Message; content: unknown },
  ) => Promise<void> {
    const self = this;

    return async function beforeProcessMessage(e) {
      const message = e.message as amqp.Message & { properties: { [MESSAGE_STORED_SPAN]: Span } };
      const msgProperties = message.properties;
      const headers = msgProperties.headers;
      const parentContext = propagation.extract(context.active(), headers);

      const span = self.tracer.startSpan(
        `${e.queue} receive`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            ...this.connection[CONNECTION_ATTRIBUTES],
            'messaging.destination.name': e.message.fields.exchange,
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

      if (self._config.subscribeHook) {
        safeExecuteInTheMiddle(
          () => self._config.subscribeHook!(span, e),
          (e) => {
            if (e) {
              diag.error('arnavmq instrumentation: subscribeHook error', e);
            }
          },
          true,
        );
      }
      message.properties[MESSAGE_STORED_SPAN] = span;
    };
  }

  getAfterProcessMessageHook(): (e: {
    message: amqp.Message;
    content: unknown;
    error?: Error;
    rejectError?: Error;
    ackError?: Error;
  }) => Promise<void> {
    return async function afterProcessMessage(e) {
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
    };
  }

  getBeforeRpcReplyHook(): (e: {
    receiveProperties: amqp.MessageProperties;
    replyProperties: amqp.MessageProperties;
    queue: string;
    reply: unknown;
    serializedReply: Buffer;
  }) => Promise<void> {
    const self = this;

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

      if (self._config.rpcResponseHook) {
        safeExecuteInTheMiddle(
          () => self._config.rpcResponseHook!(span, e),
          (e) => {
            if (e) {
              diag.error('arnavmq instrumentation: subscribeHook error', e);
            }
          },
          true,
        );
      }
      receiveProperties[MESSAGE_RPC_REPLY_STORED_SPAN] = span;
    };
  }

  getAfterRpcReplyHook(): AfterRpcHook {
    return async function afterRpcReply(e) {
      const receiveProperties = e.receiveProperties as amqp.MessageProperties & {
        [MESSAGE_RPC_REPLY_STORED_SPAN]: Span;
      };
      const span = receiveProperties[MESSAGE_RPC_REPLY_STORED_SPAN];

      if (e.error) {
        span.recordException(e.error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: `rpc reply failed: ${e.error.message}` });
      }
      span.end();
    };
  }

  getBeforePublishHook(): (
    this: { connection: InstrumentedConnection },
    e: {
      queue: string;
      message: unknown;
      parsedMessage: Buffer;
      properties: ProduceSettings;
      currentRetry: number;
    },
  ) => Promise<void> {
    const self = this;

    return async function beforePublish(e) {
      const msgProperties = e.properties as typeof e.properties & {
        [MESSAGE_PUBLISH_ATTEMPT_SPAN]: Span;
        [MESSAGE_PUBLISH_ROOT_SPAN]: Span;
      };

      let exchange = DEFAULT_EXCHANGE_NAME;
      let queue = e.queue;
      if (msgProperties.routingKey) {
        exchange = e.queue;
        queue = msgProperties.routingKey;
      }

      let parentSpan = msgProperties[MESSAGE_PUBLISH_ROOT_SPAN];
      if (!parentSpan) {
        parentSpan = self.tracer.startSpan(`${exchange} -> ${queue} send${msgProperties.rpc ? ' rpc' : ''}`, {
          kind: SpanKind.CLIENT,
          attributes: {
            ...this.connection[CONNECTION_ATTRIBUTES],
            'messaging.destination.name': exchange,
            'messaging.rabbitmq.destination.routing_key': queue,
            'messaging.rabbitmq.message.rpc': !!msgProperties.rpc,
            'messaging.message.conversation_id': msgProperties.correlationId,
            'messaging.message.id': msgProperties.messageId,
            'messaging.operation': 'publish',
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
          },
        },
        parentContext,
      );
      msgProperties.headers = msgProperties.headers || {};

      propagation.inject(trace.setSpan(parentContext, span), msgProperties.headers);

      if (self._config.publishHook) {
        safeExecuteInTheMiddle(
          () => self._config.publishHook!(span, e),
          (e) => {
            if (e) {
              diag.error('arnavmq instrumentation: publishHook error', e);
            }
          },
          true,
        );
      }

      msgProperties[MESSAGE_PUBLISH_ATTEMPT_SPAN] = span;
    };
  }

  getAfterPublishHook(): AfterPublishHook {
    return async function afterPublish(e) {
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
    };
  }
}
