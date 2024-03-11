import { NodeSDK } from '@opentelemetry/sdk-node';

const sdk = new NodeSDK({ serviceName: 'test opentelemetry' });

sdk.start();
