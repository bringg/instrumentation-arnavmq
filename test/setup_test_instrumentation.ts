import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { Attributes, Span, SpanStatus } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import ArnavmqInstrumentation from '../src/arnavmq';
import { ConsumeInfo, PublishInfo, RpcInfo } from '../src/types';

export type TestableSpan = Span & {
  attributes: Attributes;
  name: string;
  ended: boolean;
  parentSpanId: string;
  status: SpanStatus;
  events: any[];
};

export type TestSpans = {
  publish: { span: TestableSpan; info: PublishInfo }[];
  subscribe: { span: TestableSpan; info: ConsumeInfo }[];
  rpc: { span: TestableSpan; info: RpcInfo }[];
};

const spans: Map<string, TestSpans> = new Map();

export function resetSpans(context: Mocha.Context) {
  spans.delete(context.currentTest!.id);
}

export function getSpans(context: Mocha.Context) {
  let testSpans = spans.get(context.currentTest!.id);
  if (!testSpans) {
    testSpans = { publish: [], subscribe: [], rpc: [] };
    spans.set(context.currentTest!.id, testSpans);
  }
  return testSpans;
}

const provider = new NodeTracerProvider();
provider.register();

registerInstrumentations({
  instrumentations: [
    new ArnavmqInstrumentation({
      subscribeHook: (span, info) => {
        spans.get(info.queue)!.subscribe.push({ span: span as TestableSpan, info });
      },
      publishHook: (span, info) => {
        spans.get(info.queue)!.publish.push({ span: span as TestableSpan, info });
      },
      rpcResponseHook: (span, info) => {
        spans.get(info.queue)!.rpc.push({ span: span as TestableSpan, info });
      },
    }),
  ],
});
