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

const test = require('node:test');
const assert = require('node:assert');

const {
  IdempotencyCache,
  idempotencyKeyFromMetadata,
  IDEMPOTENCY_KEY_HEADER
} = require('./idempotency');

function metadataWith(value) {
  return {
    get: (key) => (key === IDEMPOTENCY_KEY_HEADER ? [value] : [])
  };
}

test('idempotencyKeyFromMetadata returns valid keys', () => {
  assert.strictEqual(
    idempotencyKeyFromMetadata(metadataWith('charge-key-1')),
    'charge-key-1');
  assert.strictEqual(idempotencyKeyFromMetadata(metadataWith('not a key')), '');
  assert.strictEqual(idempotencyKeyFromMetadata(null), '');
});

test('IdempotencyCache returns the stored response for a key', () => {
  const cache = new IdempotencyCache();
  cache.set('charge-key-1', { transaction_id: 'tx-1' });

  assert.deepStrictEqual(
    cache.get('charge-key-1'),
    { transaction_id: 'tx-1' });
  assert.strictEqual(cache.get('charge-key-2'), undefined);
  cache.stop();
});

test('IdempotencyCache expires responses after the TTL', () => {
  let now = 1000;
  const cache = new IdempotencyCache({ ttlMs: 100, now: () => now });
  cache.set('charge-key-1', { transaction_id: 'tx-1' });

  now += 99;
  assert.ok(cache.get('charge-key-1'));

  now += 2;
  assert.strictEqual(cache.get('charge-key-1'), undefined);
  cache.stop();
});

test('IdempotencyCache purges expired responses', () => {
  let now = 1000;
  const cache = new IdempotencyCache({ ttlMs: 100, now: () => now });
  cache.set('charge-key-1', { transaction_id: 'tx-1' });
  cache.set('charge-key-2', { transaction_id: 'tx-2' });

  now += 50;
  cache.set('charge-key-2', { transaction_id: 'tx-2-retry' });
  now += 60;
  cache.purgeExpired();

  assert.strictEqual(cache.get('charge-key-1'), undefined);
  assert.deepStrictEqual(
    cache.get('charge-key-2'),
    { transaction_id: 'tx-2-retry' });
  cache.stop();
});
