/*
 * Copyright 2026 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const { AsyncLocalStorage } = require('node:async_hooks');
const { randomUUID } = require('node:crypto');
const grpc = require('@grpc/grpc-js');

const REQUEST_ID_HEADER = 'x-request-id';
const REQUEST_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

const storage = new AsyncLocalStorage();

function isValidRequestId (value) {
  return typeof value === 'string' &&
    !value.includes('\n') &&
    !value.includes('\r') &&
    REQUEST_ID_PATTERN.test(value);
}

function getLogger (defaultLogger) {
  const store = storage.getStore();
  return store ? store.logger : defaultLogger;
}

function withRequestContext (baseLogger, method, handler) {
  return function (call, callback) {
    const values = call.metadata.get(REQUEST_ID_HEADER);
    const incoming = values.length > 0 ? values[0] : undefined;
    const requestId = isValidRequestId(incoming) ? incoming : randomUUID();
    const requestLogger = baseLogger.child({ request_id: requestId });
    const startedAt = process.hrtime.bigint();

    let completed = false;
    const done = function (...args) {
      if (completed) {
        return;
      }
      completed = true;
      const durationMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
      if (args[0]) {
        requestLogger.error({ method, duration_ms: durationMs, error: String(args[0]) }, 'rpc failed');
      } else {
        requestLogger.info({ method, duration_ms: durationMs }, 'rpc handled');
      }
      callback(...args);
    };

    return storage.run({ requestId, logger: requestLogger }, function () {
      const metadata = new grpc.Metadata();
      metadata.set(REQUEST_ID_HEADER, requestId);
      call.sendMetadata(metadata);
      requestLogger.info({ method }, 'rpc started');
      try {
        return handler(call, done);
      } catch (err) {
        requestLogger.error({ method, error: err.message }, 'rpc threw');
        throw err;
      }
    });
  };
}

module.exports = {
  REQUEST_ID_HEADER,
  isValidRequestId,
  getLogger,
  withRequestContext
};
