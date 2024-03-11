import './bootstrap_opentelemetry';

import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { randomUUID } from 'crypto';
import ArnavmqInstrumentation from '../src/arnavmq';

registerInstrumentations({
  instrumentations: [
    // Register the instrumentation.
    // Can pass `consumeHook`, `produceHook` and `rpcResponseHook` to add custom attributes to the spans on consume, produce, and rpc reply (produce from the server back to the client).
    // Each event payload contains it's own relevant data, see the types for details.
    new ArnavmqInstrumentation({
      consumeHook: (span, info) => {
        const parsedContent = info.action.content as { request_id: string; foo: string };
        span.setAttribute('request.id', info.action.message.properties.headers?.['request-id']);
        span.setAttribute('foo', parsedContent.foo);
        console.log(info);
      },
      // produceHook: (span, info) => {},
      // rpcResponseHook: (span, info) => {}
    }),
  ],
});

const arnavmq = require('arnavmq')({ host: 'amqp://localhost' });

async function main() {
  const queue = 'test-queue';

  let requestCounter = 0;

  await arnavmq.consume(queue, async (msg: unknown, properties: unknown) => {
    requestCounter = (requestCounter + 1) % 3;
    if (!requestCounter) {
      console.log('Throwing error on consume!', msg, properties);
      throw new Error('consume error');
    }

    await new Promise((res) => {
      console.log('simulating work...');
      setTimeout(res, 2000);
    });
    console.log('Got message!', msg, properties);
    return 'Returned something!';
  });

  setInterval(async () => {
    const message = { foo: 'bar' };
    try {
      const res = await arnavmq.produce(queue, message, {
        rpc: !(requestCounter % 2),
        headers: { 'request-id': randomUUID() },
      });
      console.log(res);
    } catch (error) {
      console.log('Got error!', error);
    }
  }, 1000);
}

main();
