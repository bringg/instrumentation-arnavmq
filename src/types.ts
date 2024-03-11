import { Attributes, Span } from '@opentelemetry/api';
import { InstrumentationConfig } from '@opentelemetry/instrumentation';
import type { Connection, ConsumerHooks, ProducerHooks } from 'arnavmq';
import type { CONNECTION_ATTRIBUTES } from './consts';

export type InstrumentedConnection = Connection & {
  [CONNECTION_ATTRIBUTES]: Attributes;
};

export interface ArnavmqProduceCustomAttributeFunction {
  (span: Span, produceInfo: ProducerHooks.ProduceInfo): void;
}

export interface ArnavmqConsumeCustomAttributeFunction {
  (span: Span, consumeInfo: ConsumerHooks.ConsumeInfo): void;
}

export interface ArnavmqRpcResponseCustomAttributeFunction {
  (span: Span, produceInfo: ConsumerHooks.RpcInfo): void;
}

export interface ArnavmqInstrumentationConfig extends InstrumentationConfig {
  /** hook for adding custom attributes before a produced message is sent */
  produceHook?: ArnavmqProduceCustomAttributeFunction;

  /** hook for adding custom attributes before consumer message is processed */
  consumeHook?: ArnavmqConsumeCustomAttributeFunction;

  /** hook for adding custom attributes before returning RPC message to the producer */
  rpcResponseHook?: ArnavmqRpcResponseCustomAttributeFunction;
}
