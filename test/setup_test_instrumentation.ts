import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { Attributes, Span, SpanStatus } from '@opentelemetry/api';
import ArnavmqInstrumentation from '../src/arnavmq';
import { ProduceInfo } from 'arnavmq/types/modules/hooks/producer_hooks';
import { ConsumeInfo, RpcInfo } from 'arnavmq/types/modules/hooks/consumer_hooks';

export type TestableSpan = Span & {
  attributes: Attributes;
  name: string;
  ended: boolean;
  parentSpanId: string;
  status: SpanStatus;
  events: unknown[];
};

export type TestSpans = {
  produce: { span: TestableSpan; info: ProduceInfo }[];
  consume: { span: TestableSpan; info: ConsumeInfo }[];
  rpc: { span: TestableSpan; info: RpcInfo }[];
};

const spans: Map<string, TestSpans> = new Map();

export function resetSpans(context: Mocha.Context) {
  spans.delete(context.currentTest!.id);
}

export function getSpans(context: Mocha.Context) {
  let testSpans = spans.get(context.currentTest!.id);
  if (!testSpans) {
    testSpans = { produce: [], consume: [], rpc: [] };
    spans.set(context.currentTest!.id, testSpans);
  }
  return testSpans;
}

registerInstrumentations({
  instrumentations: [
    new ArnavmqInstrumentation({
      consumeHook: (span, info) => {
        spans.get(info.queue)!.consume.push({ span: span as TestableSpan, info });
      },
      produceHook: (span, info) => {
        spans.get(info.queue)!.produce.push({ span: span as TestableSpan, info });
      },
      rpcResponseHook: (span, info) => {
        spans.get(info.queue)!.rpc.push({ span: span as TestableSpan, info });
      },
    }),
  ],
});
