#!/usr/bin/python
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import time

IDEMPOTENCY_KEY_HEADER = 'x-idempotency-key'
IDEMPOTENCY_KEY_PATTERN = re.compile(r'^[A-Za-z0-9._:-]{1,128}$')
DEFAULT_TTL_SECONDS = 15 * 60


def idempotency_key_from_context(context):
    """Returns the first valid idempotency key from incoming gRPC metadata."""
    for item in context.invocation_metadata():
        key, value = item.key, item.value
        if key == IDEMPOTENCY_KEY_HEADER and IDEMPOTENCY_KEY_PATTERN.match(value):
            return value
    return ''


class IdempotencyCache:
    """Remembers recently processed idempotency keys."""

    def __init__(self, ttl_seconds=DEFAULT_TTL_SECONDS, now=time.time):
        self._ttl = ttl_seconds
        self._now = now
        self._entries = {}

    def seen(self, key):
        if not key:
            return False

        now = self._now()
        created_at = self._entries.get(key)
        if created_at is not None and now - created_at < self._ttl:
            return True

        self._entries[key] = now
        return False
