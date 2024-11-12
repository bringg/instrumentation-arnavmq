import { resetSpans, getSpans, TestSpans } from './setup_test_instrumentation';
import { assertSpanAttributes, produceMessage, produceRpcAndRetryTwice } from './utils';
import { DEFAULT_EXCHANGE_NAME, RPC_REPLY_DESTINATION_NAME } from '../src/consts';
import * as amq from 'arnavmq';

const arnavmq = amq({ host: 'amqp://localhost' });

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

  context('publish span', () => {
    it('creates a publish span with correct data on non-rpc publishes', async () => {
      const publishOptions = await produceMessage(queue, arnavmq, false);

      expect(testSpans.produce).to.have.lengthOf(1);
      const publishInfo = testSpans.produce[0].info;
      const publishSpan = testSpans.produce[0].span;
      assertSpanAttributes(publishSpan, queue, 'publish', publishOptions, {
        bodySize: publishInfo.parsedMessage.byteLength,
        name: `${DEFAULT_EXCHANGE_NAME} -> ${queue} publish`,
      });
    });

    it('creates a publish span with correct data on rpc', async function () {
      const publishOptions = await produceMessage(queue, arnavmq, true);

      expect(testSpans.produce).to.have.lengthOf(1);
      const publishInfo = testSpans.produce[0].info;
      const publishSpan = testSpans.produce[0].span;
      assertSpanAttributes(publishSpan, queue, 'publish', publishOptions, {
        bodySize: publishInfo.parsedMessage.byteLength,
        name: `${DEFAULT_EXCHANGE_NAME} -> ${queue} publish rpc`,
        correlationId: publishInfo.properties.correlationId,
      });
    });

    it('creates a single publish span even when consume callback fails and retries multiple times', async function () {
      const publishOptions = await produceRpcAndRetryTwice(queue, arnavmq);

      expect(testSpans.produce).to.have.lengthOf(1);
      const publishInfo = testSpans.produce[0].info;
      const publishSpan = testSpans.produce[0].span;
      assertSpanAttributes(publishSpan, queue, 'publish', publishOptions, {
        bodySize: publishInfo.parsedMessage.byteLength,
        name: `${DEFAULT_EXCHANGE_NAME} -> ${queue} publish rpc`,
        correlationId: publishInfo.properties.correlationId,
      });
    });
  });

  context('receive span', () => {
    it('creates a receive span with correct data on non-rpc publishes', async () => {
      const publishOptions = await produceMessage(queue, arnavmq, false);

      const publishInfo = testSpans.produce[0].info;
      const publishSpan = testSpans.produce[0].span;

      expect(testSpans.consume).to.have.lengthOf(1);
      const receiveInfo = testSpans.consume[0].info;
      // Wait for the next event loop just in case of a race where the "subscribe callback" finished but the "subscriber" hasn't ended the span yet, failing the `expect(span.ended)` check. It happens.
      await new Promise<unknown>((res) => {
        setImmediate(res);
      });
      assertSpanAttributes(testSpans.consume[0].span, queue, 'receive', publishOptions, {
        bodySize: receiveInfo.action.message.content.byteLength,
        name: `${queue} receive`,
        parent: publishSpan.spanContext().spanId,
      });

      expect(receiveInfo.action.message.content.byteLength).to.equal(publishInfo.parsedMessage.byteLength);
    });

    it('creates a receive span with correct data on rpc', async function () {
      const publishOptions = await produceMessage(queue, arnavmq, true);

      expect(testSpans.consume).to.have.lengthOf(1);
      const receiveSpan = testSpans.consume[0].span;
      const receiveInfo = testSpans.consume[0].info;
      assertSpanAttributes(receiveSpan, queue, 'receive', publishOptions, {
        name: `${queue} receive`,
        bodySize: receiveInfo.action.message.content.byteLength,
        correlationId: receiveInfo.action.message.properties.correlationId,
        parent: testSpans.produce[0].span.spanContext().spanId,
      });
      expect(receiveInfo.action.message.content.byteLength).to.equal(
        testSpans.produce[0].info.parsedMessage.byteLength,
      );
    });

    it('creates a receive span for each retry of an rpc consume callback', async function () {
      const publishOptions = await produceRpcAndRetryTwice(queue, arnavmq);
      const expectedAttempts = 3;
      const publishSpan = testSpans.produce[0].span;

      expect(testSpans.consume).to.have.lengthOf(expectedAttempts);
      const successReceive = testSpans.consume[testSpans.consume.length - 1];
      assertSpanAttributes(successReceive.span, queue, 'receive', publishOptions, {
        name: `${queue} receive`,
        bodySize: successReceive.info.action.message.content.byteLength,
        correlationId: successReceive.info.action.message.properties.correlationId,
        parent: publishSpan.spanContext().spanId,
      });

      const failedReceives = testSpans.consume.slice(0, -1);
      expect(failedReceives).to.have.lengthOf(expectedAttempts - 1);
      failedReceives.forEach((failedReceive, i) => {
        assertSpanAttributes(failedReceive.span, queue, 'receive', publishOptions, {
          name: `${queue} receive`,
          bodySize: failedReceive.info.action.message.content.byteLength,
          correlationId: failedReceive.info.action.message.properties.correlationId,
          error: `Test reject ${i + 1}`,
          parent: publishSpan.spanContext().spanId,
        });
      });
    });
  });

  context('rpc reply span', () => {
    it('does not create an rpc reply span when not sending an rpc request', async () => {
      await produceMessage(queue, arnavmq, false);

      expect(testSpans.rpc).to.be.empty;
    });

    it('creates an rpc reply span with correct data', async function () {
      await produceMessage(queue, arnavmq, true);

      const publishInfo = testSpans.produce[0].info;
      const receiveInfo = testSpans.consume[0].info;
      const receiveSpan = testSpans.consume[0].span;

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

      // All spans should have the same rpc correlation id.
      expect(rpcInfo.receiveProperties.correlationId)
        .to.equal(rpcInfo.replyProperties.correlationId)
        .and.equal(publishInfo.properties.correlationId)
        .and.equal(receiveInfo.action.message.properties.correlationId);
    });

    it('creates a single rpc reply span only for the successful consume callback when consume failed and retried', async function () {
      await produceRpcAndRetryTwice(queue, arnavmq);

      const publishInfo = testSpans.produce[0].info;
      const successReceive = testSpans.consume[testSpans.consume.length - 1];

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
        .and.equal(successReceive.info.action.message.properties.correlationId);
    });
  });
});
