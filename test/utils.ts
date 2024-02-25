import { randomUUID } from 'crypto';
import { Attributes, SpanStatusCode } from '@opentelemetry/api';
import { TestableSpan } from './setup_test_instrumentation';
import { ProduceSettings } from '../src/types';
import { DEFAULT_EXCHANGE_NAME } from '../src/consts';

export async function publishMessage(queue: string, arnavmq: any, rpc: boolean) {
  let resolveReceivedPromise: (value: unknown) => void;
  const receivedPromise = new Promise((resolve) => {
    resolveReceivedPromise = resolve;
  });

  await arnavmq.subscribe(queue, (msg: string) => {
    resolveReceivedPromise(msg);
    return 'result!';
  });

  const publishOptions: { messageId: string; rpc?: true } = { messageId: randomUUID() };
  if (rpc) {
    publishOptions.rpc = true;
  }

  const response = await arnavmq.publish(queue, 'test message', publishOptions);
  const received = await receivedPromise;

  expect(received).to.equal('test message');
  if (rpc) {
    expect(response).to.equal('result!');
  }

  return publishOptions;
}

export async function publishRpcAndRetryTwice(queue: string, arnavmq: any) {
  let attempt = 0;
  const expectedAttempts = 3;
  let resolveReceivedPromise: (value: unknown) => void;
  const receivedPromise = new Promise((resolve) => {
    resolveReceivedPromise = resolve;
  });
  await arnavmq.subscribe(queue, (msg: string) => {
    attempt += 1;

    if (attempt === expectedAttempts) {
      resolveReceivedPromise(msg);
      return attempt;
    }
    throw new Error(`Test reject ${attempt}`);
  });
  const publishOptions = { rpc: true, messageId: randomUUID() };

  const response = await arnavmq.publish(queue, 'test message', publishOptions);
  const received = await receivedPromise;

  expect(response).to.equal(expectedAttempts);
  expect(received).to.equal('test message');

  return publishOptions;
}

export function assertSpanAttributes(
  span: TestableSpan,
  queue: string,
  operation: 'create' | 'publish' | 'receive',
  messageProperties: Partial<ProduceSettings>,
  options: Partial<{
    name: string;
    parent: string;
    error: string;
    bodySize: number;
    correlationId: string;
    temporary: boolean;
  }>,
) {
  expect(queue).not.to.be.empty;

  const expectedAttributes: Attributes = {
    'network.protocol.version': '0.9.1',
    'network.protocol.name': 'AMQP',
    'server.address': 'localhost',
    'server.port': 5672,
    'messaging.system': 'rabbitmq',
    'messaging.destination.name': DEFAULT_EXCHANGE_NAME,
    'messaging.rabbitmq.destination.routing_key': queue,
    'messaging.operation': operation,
    'messaging.rabbitmq.message.rpc': !!messageProperties.rpc,
  };

  if (messageProperties.messageId) {
    expectedAttributes['messaging.message.id'] = messageProperties.messageId;
  }

  if ('bodySize' in options) {
    expect(options.bodySize).to.be.greaterThan(0);
    expectedAttributes['messaging.message.body.size'] = options.bodySize;
  }
  if ('correlationId' in options) {
    expect(options.correlationId).to.be.a('string').and.not.be.empty;
    expectedAttributes['messaging.message.conversation_id'] = options.correlationId;
  }
  if ('temporary' in options) {
    expectedAttributes['messaging.destination.temporary'] = options.temporary;
  }
  if ('name' in options) {
    expect(span.name).to.equal(options.name);
  }
  if ('parent' in options) {
    expect(span.parentSpanId).to.equal(options.parent);
  }
  if ('error' in options) {
    expect(span.status.code).to.equal(SpanStatusCode.ERROR);
    expect(span.status.message).to.include(options.error);
    expect(span.events).to.have.lengthOf(1);
    expect(span.events[0].attributes['exception.message']).to.equal(options.error);
  } else {
    expect(span.status.code).not.to.equal(SpanStatusCode.ERROR);
  }

  expect(span.ended).to.be.true;
  expect(span.attributes).to.deep.equal(expectedAttributes);
}
