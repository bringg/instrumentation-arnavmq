import {
  InstrumentationBase,
  InstrumentationModuleDefinition,
  InstrumentationNodeModuleDefinition,
  InstrumentationNodeModuleFile,
  isWrapped,
  safeExecuteInTheMiddle,
  safeExecuteInTheMiddleAsync,
} from '@opentelemetry/instrumentation';
import {
  Attributes,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  context,
  diag,
  propagation,
  trace,
} from '@opentelemetry/api';

import {
  CONNECTION_ATTRIBUTES,
  RPC_REPLY_DESTINATION_NAME,
  getConnectionOptionsAttributes,
  getServerPropertiesAttributes,
} from './utils';
import { ArnavmqInstrumentationConfig, ConnectionOptions, InstrumentedConnection } from './types';
import type * as amqp from 'amqplib';

export class ArnavmqInstrumentation extends InstrumentationBase {
  protected override _config!: ArnavmqInstrumentationConfig;

  constructor(config?: ArnavmqInstrumentationConfig) {
    // TODO: Generate a version file on ci to read the version from.
    super('instrumentation-arnavmq', '0.0.1', config);
  }

  protected override init(): void | InstrumentationModuleDefinition<any> | InstrumentationModuleDefinition<any>[] {
    const supportedVersions = ['>=0.15.2'];

    const connectionInstrumentation = new InstrumentationNodeModuleFile(
      'arnavmq/src/modules/connection.js',
      supportedVersions,
      this.patchConnection.bind(this),
      this.unpatchConnection.bind(this),
    );
    const producerInstrumentation = new InstrumentationNodeModuleFile(
      'arnavmq/src/modules/producer.js',
      supportedVersions,
      this.patchProducer.bind(this),
      this.unpatchProducer.bind(this),
    );
    const consumerInstrumentation = new InstrumentationNodeModuleFile(
      'arnavmq/src/modules/consumer.js',
      supportedVersions,
      this.patchConsumer.bind(this),
      this.unpatchConsumer.bind(this),
    );

    return new InstrumentationNodeModuleDefinition('arnavmq', supportedVersions, undefined, undefined, [
      connectionInstrumentation,
      producerInstrumentation,
      consumerInstrumentation,
    ]);
  }

  patchConnection(moduleExports: any) {
    if (isWrapped(moduleExports.Connection.prototype.getConnection)) {
      return moduleExports;
    }
    this._wrap(moduleExports.Connection.prototype, 'getConnection', (original: Function) => {
      return async function getInstrumentedConnection(this: {
        config: ConnectionOptions;
        [CONNECTION_ATTRIBUTES]: Attributes;
      }) {
        // Awaiting the original will alter the call stack, but I must do it to add the attributes to the connection.
        const conn: InstrumentedConnection = await original.call(this);
        if (conn[CONNECTION_ATTRIBUTES]) {
          // getConnection Initializes the connection lazily and returns the same one until it is closed, so the attributes should only be set on it once.
          return conn;
        }

        const optionsAttributes = getConnectionOptionsAttributes(this.config);
        const serverPropertiesAttributes = getServerPropertiesAttributes(conn.connection);
        this[CONNECTION_ATTRIBUTES] = { ...optionsAttributes, ...serverPropertiesAttributes };

        return conn;
      };
    });

    return moduleExports;
  }
  unpatchConnection(moduleExports?: any) {
    if (isWrapped(moduleExports.Connection.prototype.getConnection)) {
      this._unwrap(moduleExports.Connection.prototype, 'getConnection');
    }
    return moduleExports;
  }

  patchProducer(moduleExports: unknown, moduleVersion?: string | undefined) {
    // TODO: implement
    return moduleExports;
  }
  unpatchProducer(moduleExports?: unknown, moduleVersion?: string | undefined) {
    // TODO: implement
    return moduleExports;
  }

  patchConsumer(moduleExports: any, moduleVersion?: string | undefined) {
    if (!isWrapped(moduleExports.prototype.subscribe)) {
      this._wrap(moduleExports.prototype, 'subscribe', this.getSubscribePatch.bind(this, moduleVersion));
    }
    if (!isWrapped(moduleExports.prototype.checkRpc)) {
      this._wrap(moduleExports.prototype, 'checkRpc', this.getCheckRpcPatch.bind(this, moduleVersion));
    }

    return moduleExports;
  }
  unpatchConsumer(moduleExports?: any, moduleVersion?: string | undefined) {
    if (isWrapped(moduleExports.prototype.subscribe)) {
      this._unwrap(moduleExports.prototype, 'subscribe');
    }
    if (isWrapped(moduleExports.prototype.checkRpc)) {
      this._unwrap(moduleExports.prototype, 'checkRpc');
    }
    return moduleExports;
  }

  getSubscribePatch(moduleVersion: string | undefined, original: Function) {
    const self = this;

    return function subscribe(
      this: any,
      queue: string,
      options: { noAck: boolean } | ((body: unknown, msgProperties: amqp.MessageProperties) => Promise<unknown>),
      callback?: (body: unknown, msgProperties: amqp.MessageProperties) => Promise<unknown>,
    ) {
      let callbackArg = 2;
      // In case using the callback only overload
      if (typeof options === 'function') {
        callbackArg = 1;
        callback = options;
      }

      const consumer = this;
      const instrumentedCallback = function instrumentedCallback(
        body: unknown,
        msgProperties: amqp.MessageProperties,
      ): Promise<unknown> {
        msgProperties = msgProperties || {};
        const headers = msgProperties.headers;
        const parentContext = propagation.extract(ROOT_CONTEXT, headers);

        const span = self.tracer.startSpan(
          `${queue} deliver`,
          {
            kind: SpanKind.CONSUMER,
            attributes: {
              ...consumer.connection[CONNECTION_ATTRIBUTES],
              'messaging.destination.name': queue,
              'messaging.message.id': msgProperties.messageId,
              'messaging.operation': 'deliver',
              'messaging.message.conversation_id': msgProperties.correlationId,
              // We already get the deserialized message, so can't reliably check the message size with Buffer.byteLength
              // 'messaging.message.body.size': Buffer.byteLength(serializedMessage)
            },
          },
          parentContext,
        );

        if (self._config.subscribeHook) {
          safeExecuteInTheMiddle(
            () => self._config.subscribeHook!(span, { moduleVersion, body }),
            (e) => {
              if (e) {
                diag.error('arnavmq instrumentation: subscribeHook error', e);
              }
            },
            true,
          );
        }

        // Execute the original callback within the span, record (and rethrow) the error if any, and always finish the span.
        // The span does not include the `ack` or `reject` actions, which are made after the callback is executed according to it's results.
        return safeExecuteInTheMiddleAsync(
          () => context.with(trace.setSpan(parentContext, span), () => callback!(body, msgProperties)),
          (e) => {
            if (e) {
              span.recordException(e);
              span.setStatus({
                code: SpanStatusCode.ERROR,
                message: `subscribe callback message processing failed: ${e.message}`,
              });
            }
            span.end();
          },
        );
      };

      arguments[callbackArg] = instrumentedCallback;
      return original.apply(consumer, arguments);
    };
  }

  getCheckRpcPatch(moduleVersion: string | undefined, original: Function) {
    const self = this;

    return function checkRpc(this: any, messageProperties: amqp.MessageProperties, queue: string, reply: unknown) {
      const consumer = this;

      if (!messageProperties.replyTo) {
        return original.apply(consumer, arguments);
      }

      const replyTo = messageProperties.replyTo;
      const correlationId = messageProperties.correlationId;
      const span = self.tracer.startSpan(`${queue} -> ${RPC_REPLY_DESTINATION_NAME} publish`, {
        kind: SpanKind.PRODUCER,
        attributes: {
          ...consumer.connection[CONNECTION_ATTRIBUTES],
          'messaging.destination.name': replyTo,
          'messaging.destination.temporary': true,
          'messaging.message.conversation_id': correlationId,
          'messaging.operation': 'publish',
        },
      });
      // Not adding propagation to the reply headers, since the headers are expected to always be empty

      if (self._config.rpcResponseHook) {
        safeExecuteInTheMiddle(
          () => self._config.rpcResponseHook!(span, { moduleVersion, reply, options: { correlationId } }),
          (e) => {
            if (e) {
              diag.error('arnavmq instrumentation: rpcResponseHook error', e);
            }
          },
          true,
        );
      }

      // Execute the original publish within the span, record (and rethrow) the error if any, and always finish the span.
      return safeExecuteInTheMiddleAsync(
        () => context.with(trace.setSpan(context.active(), span), () => original.apply(consumer, arguments)),
        (e) => {
          if (e) {
            span.recordException(e);
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: `rpc reply failed: ${e.message}`,
            });
          }
          span.end();
        },
      );
    };
  }
}
