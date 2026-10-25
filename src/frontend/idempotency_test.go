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

package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestValidIdempotencyKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{name: "uuid", key: "6f0f4e3a-21d0-4a5e-91e0-2f4f4f2f7a11", want: true},
		{name: "short", key: "abc", want: true},
		{name: "empty", key: "", want: false},
		{name: "spaces", key: "not a key", want: false},
		{name: "too long", key: string(make([]byte, 129)), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := validIdempotencyKey(tc.key); got != tc.want {
				t.Errorf("validIdempotencyKey(%q) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

func TestNewIdempotencyKeyIsValid(t *testing.T) {
	if key := newIdempotencyKey(); !validIdempotencyKey(key) {
		t.Errorf("generated key %q is not valid", key)
	}
}

func TestWithIdempotencyKeySetsOutgoingMetadata(t *testing.T) {
	ctx := withIdempotencyKey(context.Background(), "checkout-key-1")

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	got := md.Get(idempotencyKeyHeader)
	if len(got) != 1 || got[0] != "checkout-key-1" {
		t.Errorf("metadata %q = %v, want [checkout-key-1]", idempotencyKeyHeader, got)
	}
}
