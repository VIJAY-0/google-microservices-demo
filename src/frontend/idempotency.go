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
	"net/http"
	"regexp"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const idempotencyKeyHeader = "x-idempotency-key"

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func newIdempotencyKey() string {
	return uuid.NewString()
}

func validIdempotencyKey(key string) bool {
	return idempotencyKeyPattern.MatchString(key)
}

func idempotencyKeyFromRequest(r *http.Request) string {
	key := r.FormValue("idempotency-key")
	if validIdempotencyKey(key) {
		return key
	}
	return newIdempotencyKey()
}

func withIdempotencyKey(ctx context.Context, key string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, idempotencyKeyHeader, key)
}
