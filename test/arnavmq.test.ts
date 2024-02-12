import { randomUUID } from 'crypto';
import { resetSpans, getSpans, TestSpans } from './setup_test_instrumentation';
import assertSpanAttributes from './assert_span_attributes';
import { DEFAULT_EXCHANGE_NAME, RPC_REPLY_DESTINATION_NAME } from '../src/consts';

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
    await arnavmq.publish(queue, 'test message', publishOptions);
    const received = await receivedPromise;

    expect(received).to.equal('test message');

    // Check publish span
    expect(testSpans.publish).to.have.lengthOf(1);
    const publishInfo = testSpans.publish[0].info;
    assertSpanAttributes(testSpans.publish[0].span, queue, 'create', publishOptions, {
      bodySize: publishInfo.parsedMessage.byteLength,
      name: `${DEFAULT_EXCHANGE_NAME} -> ${queue} create`,
    });

    // Check subscribe span
    expect(testSpans.subscribe).to.have.lengthOf(1);
    const receiveInfo = testSpans.subscribe[0].info;
    // Wait for the next event loop just in case of a race where the "subscribe callback" finished but the "subscriber" hasn't ended the span yet, failing the `expect(span.ended)` check. It happens.
    await new Promise<unknown>((res) => {
      setImmediate(res);
    });
    assertSpanAttributes(testSpans.subscribe[0].span, queue, 'receive', publishOptions, {
      bodySize: receiveInfo.message.content.byteLength,
      name: `${queue} receive`,
    });

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

    const response = await arnavmq.publish(queue, 'test message', publishOptions);
    const received = await receivedPromise;

    expect(response).to.equal('result!');
    expect(received).to.equal('test message');

    // Check publish span
    expect(testSpans.publish).to.have.lengthOf(1);
    const publishInfo = testSpans.publish[0].info;
    assertSpanAttributes(testSpans.publish[0].span, queue, 'create', publishOptions, {
      bodySize: publishInfo.parsedMessage.byteLength,
      name: `${DEFAULT_EXCHANGE_NAME} -> ${queue} create rpc`,
      correlationId: publishInfo.properties.correlationId,
    });

    // Check subscribe span
    expect(testSpans.subscribe).to.have.lengthOf(1);
    const receiveSpan = testSpans.subscribe[0].span;
    const receiveInfo = testSpans.subscribe[0].info;
    assertSpanAttributes(receiveSpan, queue, 'receive', publishOptions, {
      name: `${queue} receive`,
      bodySize: receiveInfo.message.content.byteLength,
      correlationId: receiveInfo.message.properties.correlationId,
    });
    expect(receiveInfo.message.content.byteLength).to.equal(publishInfo.parsedMessage.byteLength);

    // Check RPC reply span
    expect(testSpans.rpc).to.have.lengthOf(1);
    const rpcInfo = testSpans.rpc[0].info;
    assertSpanAttributes(
      testSpans.rpc[0].span,
      publishInfo.properties.replyTo,
      'publish',
      { rpc: true },
      {
        name: `${queue} -> ${RPC_REPLY_DESTINATION_NAME} publish`,
        bodySize: rpcInfo.serializedReply.byteLength,
        correlationId: rpcInfo.receiveProperties.correlationId,
        temporary: true,
        parent: receiveSpan.spanContext().spanId,
      },
    );

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

    const response = await arnavmq.publish(queue, 'test message', publishOptions);
    const received = await receivedPromise;

    expect(response).to.equal(expectedAttempts);
    expect(received).to.equal('test message');

    // Check publish span
    expect(testSpans.publish).to.have.lengthOf(1);
    const publishInfo = testSpans.publish[0].info;
    assertSpanAttributes(testSpans.publish[0].span, queue, 'create', publishOptions, {
      bodySize: publishInfo.parsedMessage.byteLength,
      name: `${DEFAULT_EXCHANGE_NAME} -> ${queue} create rpc`,
      correlationId: publishInfo.properties.correlationId,
    });

    // Check subscribe span
    expect(testSpans.subscribe).to.have.lengthOf(expectedAttempts);

    const successReceive = testSpans.subscribe[testSpans.subscribe.length - 1];
    assertSpanAttributes(successReceive.span, queue, 'receive', publishOptions, {
      name: `${queue} receive`,
      bodySize: successReceive.info.message.content.byteLength,
      correlationId: successReceive.info.message.properties.correlationId,
    });

    const failedReceives = testSpans.subscribe.slice(0, -1);
    expect(failedReceives).to.have.lengthOf(expectedAttempts - 1);
    failedReceives.forEach((failedReceive, i) => {
      assertSpanAttributes(failedReceive.span, queue, 'receive', publishOptions, {
        name: `${queue} receive`,
        bodySize: failedReceive.info.message.content.byteLength,
        correlationId: failedReceive.info.message.properties.correlationId,
        error: `Test reject ${i + 1}`,
      });
    });

    // Check RPC reply span
    expect(testSpans.rpc).to.have.lengthOf(1);
    const rpcInfo = testSpans.rpc[0].info;
    assertSpanAttributes(
      testSpans.rpc[0].span,
      publishInfo.properties.replyTo,
      'publish',
      { rpc: true },
      {
        name: `${queue} -> ${RPC_REPLY_DESTINATION_NAME} publish`,
        bodySize: rpcInfo.serializedReply.byteLength,
        correlationId: rpcInfo.receiveProperties.correlationId,
        temporary: true,
        parent: successReceive.span.spanContext().spanId,
      },
    );

    // All spans should have the same correlation id.
    expect(rpcInfo.receiveProperties.correlationId)
      .to.equal(rpcInfo.replyProperties.correlationId)
      .and.equal(publishInfo.properties.correlationId)
      .and.equal(successReceive.info.message.properties.correlationId);
  });
});
