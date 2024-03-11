module.exports = {
  spec: 'test/*.test.ts',
  timeout: 30000,
  recursive: true,
  exit: true,
  require: [
    'chai/register-expect.js',
    'test/bootstrap_opentelemetry.ts',
    'test/setup_test_instrumentation.ts',
    'test/env.js',
  ],
  extension: 'ts',
  parallel: false,
};
