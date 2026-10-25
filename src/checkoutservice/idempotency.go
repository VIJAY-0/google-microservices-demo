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
	"os"
	"regexp"
	"sync"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	"google.golang.org/grpc/metadata"
)

const (
	idempotencyKeyHeader  = "x-idempotency-key"
	defaultIdempotencyTTL = 15 * time.Minute
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type idempotencyKeyCtxKey struct{}

func idempotencyTTL() time.Duration {
	raw := os.Getenv("IDEMPOTENCY_TTL")
	if raw == "" {
		return defaultIdempotencyTTL
	}
	ttl, _ := time.ParseDuration(raw)
	return ttl
}

func idempotencyKeyFromIncomingMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, key := range md.Get(idempotencyKeyHeader) {
		if idempotencyKeyPattern.MatchString(key) {
			return key
		}
	}
	return ""
}

func withIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idempotencyKeyCtxKey{}, key)
}

func idempotencyKeyFromContext(ctx context.Context) string {
	key, _ := ctx.Value(idempotencyKeyCtxKey{}).(string)
	return key
}

func outgoingIdempotencyContext(ctx context.Context) context.Context {
	key := idempotencyKeyFromContext(ctx)
	if key == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, idempotencyKeyHeader, key)
}

type idempotencyEntry struct {
	done      chan struct{}
	response  *pb.PlaceOrderResponse
	err       error
	createdAt time.Time
}

type idempotencyStore struct {
	mu      sync.Mutex
	entries map[string]*idempotencyEntry
	ttl     time.Duration
	now     func() time.Time
}

func newIdempotencyStore(ttl time.Duration) *idempotencyStore {
	s := &idempotencyStore{
		entries: make(map[string]*idempotencyEntry),
		ttl:     ttl,
		now:     time.Now,
	}
	go s.cleanupLoop()
	return s
}

// Do runs fn at most once per key for the lifetime of the cache entry.
// Concurrent callers with the same key wait for the first call to finish.
func (s *idempotencyStore) Do(ctx context.Context, key string, fn func() (*pb.PlaceOrderResponse, error)) (*pb.PlaceOrderResponse, error) {
	if key == "" {
		return fn()
	}

	s.mu.Lock()
	s.purgeExpiredLocked(s.now())
	if entry, ok := s.entries[key]; ok {
		s.mu.Unlock()
		<-entry.done
		return entry.response, entry.err
	}

	entry := &idempotencyEntry{
		done:      make(chan struct{}),
		createdAt: s.now(),
	}
	s.entries[key] = entry
	s.mu.Unlock()

	response, err := fn()

	// Cache the outcome under the key so retries do not repeat side effects.
	s.mu.Lock()
	entry.response = response
	entry.err = err
	s.mu.Unlock()
	close(entry.done)

	return response, err
}

func (s *idempotencyStore) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		s.purgeExpiredLocked(s.now())
		s.mu.Unlock()
	}
}

func (s *idempotencyStore) purgeExpiredLocked(now time.Time) {
	for key, entry := range s.entries {
		if now.Sub(entry.createdAt) >= s.ttl {
			delete(s.entries, key)
		}
	}
}
