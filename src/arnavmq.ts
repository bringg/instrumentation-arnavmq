import {
  InstrumentationBase,
  InstrumentationModuleDefinition,
  InstrumentationNodeModuleDefinition,
} from '@opentelemetry/instrumentation';

import { ArnavmqInstrumentationConfig, ArnavmqModule, ConnectionConfig } from './types';
import INSTRUMENTATION_ARNAVMQ_VERSION from '../version';
import {
  afterConnectHook,
  afterProcessMessageHook,
  afterPublishCallback,
  afterRpcReplyHook,
  getBeforeProcessMessageHook,
  getBeforePublishHook,
  getBeforeRpcReplyHook,
} from './instrumentation_hooks';

export default class ArnavmqInstrumentation extends InstrumentationBase {
  protected override _config!: ArnavmqInstrumentationConfig;

  private _patchedModule: boolean;

  constructor(config?: ArnavmqInstrumentationConfig) {
    super('instrumentation-arnavmq', INSTRUMENTATION_ARNAVMQ_VERSION, config);
    this._patchedModule = false;
  }

  protected override init():
    | void
    | InstrumentationModuleDefinition<ArnavmqModule>
    | InstrumentationModuleDefinition<ArnavmqModule>[] {
    // No unpatching, since we wrap the entire module export and can't unwrap it.
    // Supports version 16.0 and ahead since it relies on the hooks added there.
    return new InstrumentationNodeModuleDefinition('arnavmq', ['>=0.16.0'], this.patchArnavmq.bind(this));
  }

  patchArnavmq(moduleExports: ArnavmqModule) {
    if (this._patchedModule) {
      return moduleExports;
    }

    const arnavmqFactory: ArnavmqModule = (config: ConnectionConfig) => {
      const arnavmq = moduleExports(config);
      const { hooks } = arnavmq;
      hooks.connection.afterConnect(afterConnectHook);
      hooks.consumer.beforeProcessMessage(getBeforeProcessMessageHook(this._config, this.tracer));
      hooks.consumer.afterProcessMessage(afterProcessMessageHook);
      hooks.consumer.beforeRpcReply(getBeforeRpcReplyHook(this._config, this.tracer));
      hooks.consumer.afterRpcReply(afterRpcReplyHook);
      hooks.producer.beforePublish(getBeforePublishHook(this._config, this.tracer));
      hooks.producer.afterPublish(afterPublishCallback);

      return arnavmq;
    };

    this._patchedModule = true;
    return arnavmqFactory;
  }
}
