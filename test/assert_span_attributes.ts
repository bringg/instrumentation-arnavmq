import { Attributes, SpanStatusCode } from '@opentelemetry/api';
import { TestableSpan } from './setup_test_instrumentation';
import { ProduceSettings } from '../src/types';
import { DEFAULT_EXCHANGE_NAME } from '../src/consts';

export default function assertSpanAttributes(
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
