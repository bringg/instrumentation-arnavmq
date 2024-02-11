import { randomUUID } from 'crypto';
import { SpanStatusCode } from '@opentelemetry/api';
import { resetSpans, getSpans, TestSpans } from './setup_test_instrumentation';
import { DEFAULT_EXCHANGE_NAME, RPC_REPLY_DESTINATION_NAME } from '../src/utils';

const arnavmq = require('arnavmq')({ host: 'amqp://localhost' });

describe('arnavmq', function () {
  let testSpans: TestSpans;
  let queue: string;

  beforeEach(function () {
    testSpans = getSpans(this);
    queue = this.currentTest!.id;
  });

  afterEach(async function () {
    const channel = await arnavmq.connection.getDefaultChannel();
    await channel.deleteQueue(queue);
    resetSpans(this);
  });

  it('creates spans with correct data on non-rpc publishes', async () => {
    let resolveReceivedPromise: (value: unknown) => void;
    const receivedPromise = new Promise((resolve) => {
      resolveReceivedPromise = resolve;
    });

    await arnavmq.subscribe(queue, (msg: string) => {
      resolveReceivedPromise(msg);
      return 'result!';
    });

    const publishOptions = { messageId: randomUUID() };
    const expectedBaseAttributes = {
      'network.protocol.version': '0.9.1',
      'network.protocol.name': 'AMQP',
      'server.address': 'localhost',
      'server.port': 5672,
      'messaging.system': 'rabbitmq',
      'messaging.destination.name': DEFAULT_EXCHANGE_NAME,
    };

    await arnavmq.publish(queue, 'test message', publishOptions);
    const received = await receivedPromise;

    expect(received).to.equal('test message');

    // Check publish span
    expect(testSpans.publish).to.have.lengthOf(1);
    const publishSpan = testSpans.publish[0].span;
    const publishSpanAttributes = publishSpan.attributes;
    const publishInfo = testSpans.publish[0].info;
    expect(publishSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.rabbitmq.destination.routing_key': queue,
      'messaging.message.id': publishOptions.messageId,
      'messaging.operation': 'create',
      'messaging.rabbitmq.message.rpc': false,
      'messaging.message.body.size': publishInfo.parsedMessage.byteLength,
    });
    expect(publishInfo.properties.correlationId).to.be.undefined;
    expect(publishSpan.ended).to.be.true;
    expect(publishSpan.name).to.equal(`${DEFAULT_EXCHANGE_NAME} -> ${queue} create`);

    // Check subscribe span
    expect(testSpans.subscribe).to.have.lengthOf(1);
    const receiveSpan = testSpans.subscribe[0].span;
    const receiveSpanAttributes = receiveSpan.attributes;
    const receiveInfo = testSpans.subscribe[0].info;
    expect(receiveSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.operation': 'receive',
      'messaging.rabbitmq.destination.routing_key': queue,
      'messaging.message.id': publishOptions.messageId,
      'messaging.message.body.size': receiveInfo.message.content.byteLength,
    });
    expect(publishSpan.ended).to.be.true;
    expect(receiveSpan.name).to.equal(`${queue} receive`);
    expect(receiveInfo.message.content.byteLength).to.equal(publishInfo.parsedMessage.byteLength);

    // Check RPC reply span did not start
    expect(testSpans.rpc).to.be.empty;
  });

  it('creates spans with correct data on rpc', async function () {
    let resolveReceivedPromise: (value: unknown) => void;
    const receivedPromise = new Promise((resolve) => {
      resolveReceivedPromise = resolve;
    });
    await arnavmq.subscribe(queue, (msg: string) => {
      resolveReceivedPromise(msg);
      return 'result!';
    });
    const publishOptions = { rpc: true, messageId: randomUUID() };
    const expectedBaseAttributes = {
      'network.protocol.version': '0.9.1',
      'network.protocol.name': 'AMQP',
      'server.address': 'localhost',
      'server.port': 5672,
      'messaging.system': 'rabbitmq',
      'messaging.destination.name': DEFAULT_EXCHANGE_NAME,
    };

    const response = await arnavmq.publish(queue, 'test message', publishOptions);
    const received = await receivedPromise;

    expect(response).to.equal('result!');
    expect(received).to.equal('test message');

    // Check publish span
    expect(testSpans.publish).to.have.lengthOf(1);
    const publishSpan = testSpans.publish[0].span;
    const publishSpanAttributes = publishSpan.attributes;
    const publishInfo = testSpans.publish[0].info;
    expect(publishSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.rabbitmq.destination.routing_key': queue,
      'messaging.rabbitmq.message.rpc': true,
      'messaging.message.id': publishOptions.messageId,
      'messaging.operation': 'create',
      'messaging.message.body.size': publishInfo.parsedMessage.byteLength,
      'messaging.message.conversation_id': publishInfo.properties.correlationId,
    });
    expect(publishInfo.properties.correlationId).to.be.a('string').and.have.length.greaterThan(0);
    expect(publishSpan.ended).to.be.true;
    expect(publishSpan.name).to.equal(`${DEFAULT_EXCHANGE_NAME} -> ${queue} create rpc`);

    // Check subscribe span
    expect(testSpans.subscribe).to.have.lengthOf(1);
    const receiveSpan = testSpans.subscribe[0].span;
    const receiveSpanAttributes = receiveSpan.attributes;
    const receiveInfo = testSpans.subscribe[0].info;
    expect(receiveSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.operation': 'receive',
      'messaging.rabbitmq.destination.routing_key': queue,
      'messaging.message.conversation_id': receiveInfo.message.properties.correlationId,
      'messaging.message.id': publishOptions.messageId,
      'messaging.message.body.size': receiveInfo.message.content.byteLength,
    });
    expect(publishSpan.ended).to.be.true;
    expect(receiveSpan.name).to.equal(`${queue} receive`);
    expect(receiveInfo.message.content.byteLength).to.equal(publishInfo.parsedMessage.byteLength);

    // Check RPC reply span
    expect(testSpans.rpc).to.have.lengthOf(1);
    const rpcSpan = testSpans.rpc[0].span;
    const rpcSpanAttributes = rpcSpan.attributes;
    const rpcInfo = testSpans.rpc[0].info;
    expect(rpcSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.operation': 'publish',
      'messaging.rabbitmq.destination.routing_key': publishInfo.properties.replyTo,
      'messaging.destination.temporary': true,
      'messaging.message.conversation_id': rpcInfo.receiveProperties.correlationId,
      'messaging.message.body.size': rpcInfo.serializedReply.byteLength,
    });
    expect(rpcSpan.ended).to.be.true;
    expect(rpcSpan.name).to.equal(`${queue} -> ${RPC_REPLY_DESTINATION_NAME} publish`);
    expect(rpcSpan.parentSpanId).to.equal(receiveSpan.spanContext().spanId);

    // All spans should have the same correlation id.
    expect(rpcInfo.receiveProperties.correlationId)
      .to.equal(rpcInfo.replyProperties.correlationId)
      .and.equal(publishInfo.properties.correlationId)
      .and.equal(receiveInfo.message.properties.correlationId);
  });

  it('creates a span for each retry of an rpc consume', async function () {
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
    const expectedBaseAttributes = {
      'network.protocol.version': '0.9.1',
      'network.protocol.name': 'AMQP',
      'server.address': 'localhost',
      'server.port': 5672,
      'messaging.system': 'rabbitmq',
      'messaging.destination.name': DEFAULT_EXCHANGE_NAME,
    };

    const response = await arnavmq.publish(queue, 'test message', publishOptions);
    const received = await receivedPromise;

    expect(response).to.equal(expectedAttempts);
    expect(received).to.equal('test message');

    // Check publish span
    expect(testSpans.publish).to.have.lengthOf(1);
    const publishSpan = testSpans.publish[0].span;
    const publishSpanAttributes = publishSpan.attributes;
    const publishInfo = testSpans.publish[0].info;
    expect(publishSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.rabbitmq.destination.routing_key': queue,
      'messaging.rabbitmq.message.rpc': true,
      'messaging.message.id': publishOptions.messageId,
      'messaging.operation': 'create',
      'messaging.message.body.size': publishInfo.parsedMessage.byteLength,
      'messaging.message.conversation_id': publishInfo.properties.correlationId,
    });
    expect(publishInfo.properties.correlationId).to.be.a('string').and.have.length.greaterThan(0);
    expect(publishSpan.ended).to.be.true;
    expect(publishSpan.name).to.equal(`${DEFAULT_EXCHANGE_NAME} -> ${queue} create rpc`);

    // Check subscribe span
    expect(testSpans.subscribe).to.have.lengthOf(expectedAttempts);
    testSpans.subscribe.forEach((spanDetails) => {
      const receiveSpan = spanDetails.span;
      const receiveSpanAttributes = receiveSpan.attributes;
      const receiveInfo = spanDetails.info;
      expect(receiveSpanAttributes).to.deep.equal({
        ...expectedBaseAttributes,
        'messaging.operation': 'receive',
        'messaging.rabbitmq.destination.routing_key': queue,
        'messaging.message.conversation_id': receiveInfo.message.properties.correlationId,
        'messaging.message.id': publishOptions.messageId,
        'messaging.message.body.size': receiveInfo.message.content.byteLength,
      });
      expect(publishSpan.ended).to.be.true;
      expect(receiveSpan.name).to.equal(`${queue} receive`);
      expect(receiveInfo.message.content.byteLength).to.equal(publishInfo.parsedMessage.byteLength);
    });
    const failedAttempts = testSpans.subscribe.slice(0, -1);
    expect(failedAttempts).to.have.lengthOf(expectedAttempts - 1);
    failedAttempts.forEach((failedAttempt, i) => {
      expect(failedAttempt.span.status.code).to.equal(SpanStatusCode.ERROR);
      const expectedErrorMessage = `Test reject ${i + 1}`;
      expect(failedAttempt.span.status.message).to.include(expectedErrorMessage);
      expect(failedAttempt.span.events).to.have.lengthOf(1);
      expect(failedAttempt.span.events[0].attributes['exception.message']).to.equal(expectedErrorMessage);
    });

    // Check RPC reply span
    expect(testSpans.rpc).to.have.lengthOf(1);
    const rpcSpan = testSpans.rpc[0].span;
    const rpcSpanAttributes = rpcSpan.attributes;
    const rpcInfo = testSpans.rpc[0].info;
    expect(rpcSpanAttributes).to.deep.equal({
      ...expectedBaseAttributes,
      'messaging.operation': 'publish',
      'messaging.rabbitmq.destination.routing_key': publishInfo.properties.replyTo,
      'messaging.destination.temporary': true,
      'messaging.message.conversation_id': rpcInfo.receiveProperties.correlationId,
      'messaging.message.body.size': rpcInfo.serializedReply.byteLength,
    });
    expect(rpcSpan.ended).to.be.true;
    expect(rpcSpan.name).to.equal(`${queue} -> ${RPC_REPLY_DESTINATION_NAME} publish`);
    const lastProcessSpanDetails = testSpans.subscribe[2];
    expect(rpcSpan.parentSpanId).to.equal(lastProcessSpanDetails.span.spanContext().spanId);

    // All spans should have the same correlation id.
    expect(rpcInfo.receiveProperties.correlationId)
      .to.equal(rpcInfo.replyProperties.correlationId)
      .and.equal(publishInfo.properties.correlationId)
      .and.equal(lastProcessSpanDetails.info.message.properties.correlationId);
  });
});
