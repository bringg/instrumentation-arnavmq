# Instrumentation Arnavmq

An OpenTelemetry automatic instrumentation implementation, providing tracing for the [arnavmq](https://github.com/bringg/node-arnavmq) package on nodejs.

## Compatibility

Compatible with arnavmq v0.16.0 and later, as the package relies on the hooks api introduced there.

## Installation

In order to use this tracing library, you will also need to install the opentelemetry nodjes tracing package (<https://github.com/open-telemetry/opentelemetry-js/tree/main/packages/opentelemetry-sdk-trace-node>), and of course, the arnavmq package itself.

```bash
# the instrumented package
npm install --save arnavmq
# install opentelemetry nodejs tracing
npm install --save @opentelemetry/api @opentelemetry/sdk-trace-node
# Install the package itself
npm install --save instrumentation-arnavmq
```

## Usage

```javascript
const { NodeTracerProvider } = require('@opentelemetry/sdk-trace-node');
const { registerInstrumentations } = require('@opentelemetry/instrumentation');
const { ArnavmqInstrumentation } = require('instrumentation-arnavmq');

const provider = new NodeTracerProvider();
provider.register();

registerInstrumentations({
  instrumentations: [
    new ArnavmqInstrumentation(
      // You can add your own custom attributes to the span by registering a hook. Each hook is invoked with the relevant span and info about the operation. See the types for the info properties for each hook.
      {
        // called from the producer before producing a message.
        produceHook: (span, info) => {},
        // called from the consumer upon receiving a message, before invoking the "consume callback" on the message. The info includes various details about the message.
        consumeHook: (span, info) => {},
        // called from the consumer after finished processing an RPC request, before producing a reply. Called with info about both the request message and the reply message.
        rpcResponseHook: (span, info) => {},
      },
    ),
  ],
});
```

In order for the instrumentation to be collected you will also need to setup nodejs instrumentation as per the opentelemetry official guide:
<https://opentelemetry.io/docs/languages/js/getting-started/nodejs/#setup>

Also see the [example](example/index.ts).

## Spans

This implementation attempts to follow the OpenTelemetry [Semantic Conventions 1.24.0 for Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) specification when applicable, and the spans have the attributes specified there for the most part.

The created spans and their relations are described as follows, with the span types and hierarchy:

### Regular Produce-Consume

Since normally there is only a single published message, the `create` and `publish` spans will have the exact same length.

Normally this would mean there should be only a single `publish` span with now `create`, but in case of publication failure and retry, there would be multiple `publish` spans under the `create` span.

All the attributes are on the parent `create` span, with the chile `publish` spans only holding the custom `messaging.rabbitmq.message.retry_number` attribute in addition to the `messaging.operation` attribute.

```plain
 PUBLISH (root, producer)
|---------|
            RECEIVE (consumer)
           |---------|
```

### On Disconnect, Retry Produce

When configured to retry publication when disconnected from the server, Will not create a new span, but instead add events for the error and retry start when they occur.

The number of total publish retries until the produce succeeds is added to the sapn on the `messaging.rabbitmq.message.reconnect_retry_number` attribute, with each retry event having it set to the current retry.

```plain
 PUBLISH (root, producer)
|----(error, retry event)----(error, retry event)----(reconnected)--|
                                                                      RECEIVE (consumer)
                                                                     |---------|
```

### RPC

On RPC requests, the publish span is not closed until a response is received from the consumer.

A custom `"messaging.rabbitmq.message.rpc": true` attribute is added to the span.

```plaintext
 PUBLISH (root, producer, ends when replied)
|-----------------------------|
            RECEIVE (consumer)
           |---------|
                       PUBLISH (consumer, reply)
                      |-------|
```
