import { CONNECTION_ATTRIBUTES } from './utils';

import { Attributes, Span } from '@opentelemetry/api';
import { InstrumentationConfig } from '@opentelemetry/instrumentation';
import type * as amqp from 'amqplib';

export type InstrumentedConnection = amqp.Connection & { [CONNECTION_ATTRIBUTES]: Attributes };

export type ConnectionOptions = {
  /** amqp connection string */
  host: string;

  /** number of fetched messages at once on the channel */
  prefetch: number;

  /** requeue put back message into the broker if consumer crashes/trigger exception */
  requeue: boolean;

  /** time between two reconnect (ms) */
  timeout: number;

  /** the maximum number of retries when trying to send a message before throwing error when failing. If set to '0' will not retry. If set to less then '0', will retry indefinitely. */
  producerMaxRetries: number;

  /** default timeout for RPC calls. If set to '0' there will be none. */
  rpcTimeout: number;

  /**
   * suffix all queues names
   * @example service-something with suffix :ci becomes service-something:ci etc.
   */
  consumerSuffix: string;

  /** generate a hostname so we can track this connection on the broker (rabbitmq management plugin) */
  hostname: string;

  /** A logger object with a log function for each of the log levels ("debug", "info", "warn", or "error"). */
  logger: {
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

//
// BASED ON
// https://github.com/open-telemetry/opentelemetry-js-contrib/blob/c365375ce2d35c01df06c96a4faf8d5a5d9d1ec3/plugins/node/instrumentation-amqplib/src/types.ts
//
export interface PublishInfo {
  moduleVersion: string | undefined;
  /** The reply payload before serialization */
  reply: any;
  exchange?: string;
  routingKey?: string;
  options?: amqp.Options.Publish;
}

export interface ConsumeInfo {
  moduleVersion: string | undefined;
  /** The deserialized consumed payload */
  body: any;
}

export interface ArnavmqPublishCustomAttributeFunction {
  (span: Span, publishInfo: PublishInfo): void;
}

export interface ArnavmqConsumeCustomAttributeFunction {
  (span: Span, consumeInfo: ConsumeInfo): void;
}

export interface ArnavmqInstrumentationConfig extends InstrumentationConfig {
  /** hook for adding custom attributes before publish message is sent */
  publishHook?: ArnavmqPublishCustomAttributeFunction;

  /** hook for adding custom attributes before consumer message is processed */
  subscribeHook?: ArnavmqConsumeCustomAttributeFunction;

  /** hook for adding custom attributes before returning RPC message to the producer */
  rpcResponseHook?: ArnavmqPublishCustomAttributeFunction;
}
