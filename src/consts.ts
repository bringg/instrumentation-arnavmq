export const CONNECTION_ATTRIBUTES = Symbol('opentelemetry.arnavmq.connection.attributes');
export const MESSAGE_STORED_SPAN = Symbol('opentelemetry.arnavmq.message.stored-span');
export const MESSAGE_RPC_REPLY_STORED_SPAN = Symbol('opentelemetry.arnavmq.message.rpc-reply-stored-span');
export const MESSAGE_PUBLISH_ROOT_SPAN = Symbol('opentelemetry.arnavmq.message.publish-root-span');
export const MESSAGE_PUBLISH_ATTEMPT_SPAN = Symbol('opentelemetry.arnavmq.message.publish-attempt-span');
/**
 * Used in the span name instead of the generated destination of the rpc reply queue,
 * since dynamic destinations should not be included in span names.
 */
export const RPC_REPLY_DESTINATION_NAME = '(rpc reply)';
/**
 * Used for the exchange name span attribute (messaging.destination.name) when sending to the default exchange.
 * Will always use this on RPC responses, which always publish to the default exchange to the reply queue.
 * Note: The specification require it to be an empty string, but it is clearer this way.
 */

export const DEFAULT_EXCHANGE_NAME = '(default exchange)';
export const AMQP = 'AMQP';
