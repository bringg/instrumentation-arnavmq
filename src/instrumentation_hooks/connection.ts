import { Attributes } from '@opentelemetry/api';
import type * as amqp from 'amqplib';
import { CONNECTION_ATTRIBUTES, AMQP } from '../consts';
import { AfterConnectInfo, ConnectionConfig, InstrumentedConnection } from '../types';

function extractPort(url: URL, protocol: string): number {
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

function getConnectionConfigAttributes(config: ConnectionConfig): Attributes {
  const hostUrl = new URL(config.host as string);

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

const getServerPropertiesAttributes = (conn: amqp.Connection['connection']): Attributes => {
  const product = conn.serverProperties.product?.toLowerCase?.();
  if (product) {
    return {
      'messaging.system': product,
    };
  }
  return {};
};

export default async function afterConnectHook(this: InstrumentedConnection, e: AfterConnectInfo) {
  if (e.error) {
    return;
  }

  const optionsAttributes = getConnectionConfigAttributes(e.config);
  const serverPropertiesAttributes = getServerPropertiesAttributes(e.connection.connection);
  this[CONNECTION_ATTRIBUTES] = { ...optionsAttributes, ...serverPropertiesAttributes };
}
