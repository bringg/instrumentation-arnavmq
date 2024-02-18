import { Attributes, Span } from '@opentelemetry/api';
import { InstrumentationConfig } from '@opentelemetry/instrumentation';
import type * as amqp from 'amqplib';
import type { CONNECTION_ATTRIBUTES } from './consts';

export type InstrumentedConnection = {
  [CONNECTION_ATTRIBUTES]: Attributes;
  getConnection: () => Promise<amqp.Connection>;
};

export type ConnectionConfig = {
  /** amqp connection string - defaults to 'amqp://localhost' */
  host?: string;

  /** number of fetched messages at once on the channel - defaults to 5 */
  prefetch?: number;

  /** requeue put back message into the broker if consumer crashes/trigger exception - defaults to true */
  requeue?: boolean;

  /** time between two reconnect (ms) - defaults to 1000 */
  timeout?: number;

  /** the maximum number of retries when trying to send a message before throwing error when failing. If set to '0' will not retry. If set to less then '0', will retry indefinitely. Defaults to -1 */
  producerMaxRetries?: number;

  /** default timeout for RPC calls. If set to '0' there will be none. Defaults to 15000 */
  rpcTimeout?: number;

  /**
   * Suffix all queues names. Defaults to empty string.
   * @example service-something with suffix :ci becomes service-something:ci etc.
   */
  consumerSuffix: string;

  /** generate a hostname so we can track this connection on the broker (rabbitmq management plugin) - defaults to host from environment or random uuid */
  hostname?: string;

  /** A logger object with a log function for each of the log levels ("debug", "info", "warn", or "error"). */
  logger?: {
    debug: LogFunction;
    info: LogFunction;
    warn: LogFunction;
    error: LogFunction;
  };
};

type LogEvent = {
  message: string;
  error?: Error;
  params: Record<string, unknown>;
};

type LogFunction = (e: LogEvent) => void;

export type Hooks = {
  connection: ConnectionHooks;
  consumer: ConsumerHooks;
  producer: ProducerHooks;
};

interface ConnectionHooks {
  beforeConnect(callback: BeforeConnectHook): void;
  removeBeforeConnect(callback: BeforeConnectHook): void;
  afterConnect(callback: AfterConnectHook): void;
  removeAfterConnect(callback: AfterConnectHook): void;
}

interface ConsumerHooks {
  beforeProcessMessage(callback: MaybeArray<BeforeProcessHook>): void;
  removeBeforeProcessMessage(callback: MaybeArray<BeforeProcessHook>): void;

  afterProcessMessage(callback: MaybeArray<AfterProcessHook>): void;
  removeAfterProcessMessage(callback: MaybeArray<AfterProcessHook>): void;

  beforeRpcReply(callback: MaybeArray<BeforeRpcReplyHook>): void;
  removeBeforeRpcReply(callback: MaybeArray<BeforeRpcReplyHook>): void;

  afterRpcReply(callback: AfterRpcHook): void;
  removeAfterRpcReply(callback: AfterRpcHook): void;
}

interface ProducerHooks {
  beforePublish(callback: MaybeArray<BeforePublishHook>): void;
  removeBeforePublish(callback: MaybeArray<BeforePublishHook>): void;

  afterPublish(callback: AfterPublishHook): void;
  removeAfterPublish(callback: AfterPublishHook): void;
}

type MaybeArray<T> = T | T[];

export type HooksConfig = {
  connection?: {
    beforeConnect?: MaybeArray<BeforeConnectHook>;
    afterConnect?: MaybeArray<AfterConnectHook>;
  };
  consumer?: {
    beforeProcessMessage?: MaybeArray<BeforeProcessHook>;
    afterProcessMessage?: MaybeArray<AfterProcessHook>;
    beforeRpcReply?: MaybeArray<BeforeRpcReplyHook>;
    afterRpcReply?: MaybeArray<AfterRpcHook>;
  };
  producer?: {
    beforePublish?: MaybeArray<BeforePublishHook>;
    afterPublish?: MaybeArray<AfterPublishHook>;
  };
};

export type ProduceSettings = amqp.MessageProperties & {
  routingKey?: string;
  rpc?: boolean;
};

export type AfterConnectHook = (this: InstrumentedConnection, e: AfterConnectInfo) => Promise<void>;

type BeforeConnectHook = (e: { config: ConnectionConfig }) => Promise<void>;

export type AfterRpcHook = (this: { connection: InstrumentedConnection }, e: RpcResultInfo) => Promise<void>;

export type AfterPublishHook = (
  this: {
    connection: InstrumentedConnection;
  },
  e: PublishResultInfo,
) => Promise<void>;

export type BeforeProcessHook = (
  this: {
    connection: InstrumentedConnection;
  },
  e: ConsumeInfo,
) => Promise<void>;

export type AfterProcessHook = (
  this: {
    connection: InstrumentedConnection;
  },
  e: AfterConsumeInfo,
) => Promise<void>;

export type BeforeRpcReplyHook = (
  this: {
    connection: InstrumentedConnection;
  },
  e: RpcInfo,
) => Promise<void>;

export type BeforePublishHook = (
  this: {
    connection: InstrumentedConnection;
  },
  e: PublishInfo,
) => Promise<void>;

export type AfterConnectInfo = {
  config: ConnectionConfig;
} & (
  | {
      connection: amqp.Connection;
      error: undefined;
    }
  | {
      error: Error;
    }
);

export interface PublishInfo {
  /** The queue or exchange to publish to */
  queue: string;
  /** The pre-serialized message to publish */
  message: unknown;
  /** The serialized message buffer */
  parsedMessage: Buffer;
  /**
   * The publish properties and options.
   * If a "routingKey" is specified, it serves as the queue while the "queue" option represents the exchange instead. Otherwise the default exchange is used.
   */
  properties: ProduceSettings;
  /** The current retry attempt number */
  currentRetry: number;
}

export interface ConsumeInfo {
  /** The consumed queue */
  queue: string;
  action: {
    /** The raw amqplib message */
    message: amqp.Message;
    /** The deserialized message content */
    content: unknown;
    /** The callback to be executed with the message */
    callback: Function;
  };
}

export type AfterConsumeInfo = ConsumeInfo & {
  error?: Error;
  rejectError?: Error;
  ackError?: Error;
};

export interface RpcInfo {
  /** The properties of the original message we reply to */
  receiveProperties: amqp.MessageProperties;
  /** The properties added to the reply message */
  replyProperties: amqp.MessageProperties;
  /** The queue that the original message was consumed from */
  queue: string;
  /** The value to send back, before serialization. Returned from the "subscribe" callback. */
  reply: unknown;
  /** The serialized reply buffer */
  serializedReply: Buffer;
  /** The error in case of returning an error reply */
  error?: Error;
}

export type RpcResultInfo = RpcInfo &
  (
    | {
        error: Error;
      }
    | {
        error: undefined;
        written: boolean;
      }
  );

export type PublishResultInfo = PublishInfo &
  (
    | {
        result: unknown;
        error: undefined;
      }
    | {
        error: Error;
        shouldRetry: boolean;
      }
  );

export interface ArnavmqPublishCustomAttributeFunction {
  (span: Span, publishInfo: PublishInfo): void;
}

export interface ArnavmqConsumeCustomAttributeFunction {
  (span: Span, consumeInfo: ConsumeInfo): void;
}

export interface ArnavmqRpcResponseCustomAttributeFunction {
  (span: Span, publishInfo: RpcInfo): void;
}

export interface ArnavmqInstrumentationConfig extends InstrumentationConfig {
  /** hook for adding custom attributes before publish message is sent */
  publishHook?: ArnavmqPublishCustomAttributeFunction;

  /** hook for adding custom attributes before consumer message is processed */
  subscribeHook?: ArnavmqConsumeCustomAttributeFunction;

  /** hook for adding custom attributes before returning RPC message to the producer */
  rpcResponseHook?: ArnavmqRpcResponseCustomAttributeFunction;
}

export type ArnavmqModule = (config: ConnectionConfig) => {
  hooks: Hooks;
  /** ... more stuff I don't need for instrumentation */
};
