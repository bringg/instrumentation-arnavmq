import './bootstrap_opentelemetry';

import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { ArnavmqInstrumentation } from '../src/arnavmq';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { randomUUID } from 'crypto';

const provider = new NodeTracerProvider();
provider.register();

registerInstrumentations({
  instrumentations: [
    new ArnavmqInstrumentation({
      subscribeHook: (span, info) => {
        span.setAttribute('foo', info.body.foo);
        console.log(info);
      },
      // publishHook: (span: Span, publishInfo: PublishInfo) => { },
      // publishConfirmHook: (span: Span, publishConfirmedInto: PublishConfirmedInfo) => { },
      // consumeHook: (span: Span, consumeInfo: ConsumeInfo) => { },
      // consumeEndHook: (span: Span, consumeEndInfo: ConsumeEndInfo) => { },
    }),
  ],
});

const arnavmq = require('arnavmq')({ host: 'amqp://localhost' });

async function main() {
  const queue = 'test-queue';
  const message = {
    foo: 'bar',
  };

  let requestCounter = 1;

  await arnavmq.subscribe(queue, (msg: typeof message, properties: unknown) => {
    requestCounter = (requestCounter + 1) % 3;
    if (!requestCounter) {
      console.log('Throwing error on consume!', msg, properties);
      throw new Error('consume error');
    }
    console.log('Got message!', msg, properties);
    return 'Returned something!';
  });

  setInterval(async () => {
    try {
      const res = await arnavmq.publish(queue, message, {
        rpc: !(requestCounter % 2),
        headers: { 'x-request-id': randomUUID() },
      });
      console.log(res);
    } catch (error) {
      console.log('Got error!', error);
    }
  }, 5000);
}

main();
