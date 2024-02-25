import './bootstrap_opentelemetry';

import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { randomUUID } from 'crypto';
import ArnavmqInstrumentation from '../src/arnavmq';

const provider = new NodeTracerProvider();
provider.register();

registerInstrumentations({
  instrumentations: [
    new ArnavmqInstrumentation({
      consumeHook: (span, info) => {
        const parsedContent = info.action.content as { request_id: string; foo: string };
        span.setAttribute('request_id', parsedContent.request_id);
        span.setAttribute('foo', parsedContent.foo);
        console.log(info);
      },
    }),
  ],
});

const arnavmq = require('arnavmq')({ host: 'amqp://localhost' });

function getMessage() {
  return {
    foo: 'bar',
    request_id: randomUUID(),
  };
}

async function main() {
  const queue = 'test-queue';

  let requestCounter = 1;

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
    try {
      const res = await arnavmq.produce(queue, getMessage(), {
        rpc: !(requestCounter % 2),
        headers: { 'x-request-id': randomUUID() },
      });
      console.log(res);
    } catch (error) {
      console.log('Got error!', error);
    }
  }, 1000);
}

main();
