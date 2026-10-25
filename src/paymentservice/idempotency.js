// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

const IDEMPOTENCY_KEY_HEADER = 'x-idempotency-key';
const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;
const DEFAULT_TTL_MS = 15 * 60 * 1000;
const PURGE_INTERVAL_MS = 60 * 1000;

/**
 * Returns the first well-formed idempotency key in gRPC metadata.
 *
 * @param {grpc.Metadata} metadata
 * @return {string}
 */
function idempotencyKeyFromMetadata(metadata) {
  if (!metadata) {
    return '';
  }
  const values = metadata.get(IDEMPOTENCY_KEY_HEADER) || [];
  for (const value of values) {
    if (IDEMPOTENCY_KEY_PATTERN.test(value)) {
      return value;
    }
  }
  return '';
}

/**
 * Bounded TTL cache of successful charge responses, keyed by idempotency key.
 */
class IdempotencyCache {
  constructor({ ttlMs = DEFAULT_TTL_MS, now = Date.now } = {}) {
    this.ttlMs = ttlMs;
    this.now = now;
    this.entries = new Map();
    this.timer = setInterval(() => this.purgeExpired(), PURGE_INTERVAL_MS);
    if (typeof this.timer.unref === 'function') {
      this.timer.unref();
    }
  }

  get(key) {
    const entry = this.entries.get(key);
    if (!entry) {
      return undefined;
    }
    if (this.now() - entry.createdAt >= this.ttlMs) {
      this.entries.delete(key);
      return undefined;
    }
    return entry.response;
  }

  set(key, response) {
    this.entries.set(key, { response, createdAt: this.now() });
  }

  purgeExpired() {
    const now = this.now();
    for (const [key, entry] of this.entries) {
      if (now - entry.createdAt >= this.ttlMs) {
        this.entries.delete(key);
      }
    }
  }

  stop() {
    clearInterval(this.timer);
  }
}

module.exports = {
  IdempotencyCache,
  idempotencyKeyFromMetadata,
  IDEMPOTENCY_KEY_HEADER
};
