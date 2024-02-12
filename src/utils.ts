import { Attributes } from '@opentelemetry/api';
import type * as amqp from 'amqplib';
import { ConnectionConfig } from './types';

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

const AMQP = 'AMQP';

function extractPort(url: URL, protocol: string) {
  if (url.port.length) {
    return Number(url.port);
  }

  if (protocol === AMQP) {
    // AMQP default port
    return 5672;
  }

  // AMQPS default port
  return 5671;
}

export function getConnectionConfigAttributes(config: ConnectionConfig): Attributes {
  const hostUrl = new URL(config.host);

  // Trim the ':' from the url protocol and uppercase.
  // Should be either AMQP or AMQPS
  const protocolAttribute = hostUrl.protocol.length
    ? hostUrl.protocol.substring(0, hostUrl.protocol.length - 1).toUpperCase()
    : AMQP;

  const attributes: Attributes = {
    // The amqplib supports only the AMQP 0-9-1 protocol specification.
    'network.protocol.version': '0.9.1',
    'network.protocol.name': protocolAttribute,
    'server.address': hostUrl.hostname,
    'server.port': extractPort(hostUrl, protocolAttribute),
  };

  return attributes;
}

export const getServerPropertiesAttributes = (conn: amqp.Connection['connection']): Attributes => {
  const product = conn.serverProperties.product?.toLowerCase?.();
  if (product) {
    return {
      'messaging.system': product,
    };
  }
  return {};
};
