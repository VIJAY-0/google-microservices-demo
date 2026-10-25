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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	"google.golang.org/grpc/metadata"
)

func newTestIdempotencyStore(ttl time.Duration) *idempotencyStore {
	return &idempotencyStore{
		entries: make(map[string]*idempotencyEntry),
		ttl:     ttl,
		now:     time.Now,
	}
}

func successfulOrder(orderID string) *pb.PlaceOrderResponse {
	return &pb.PlaceOrderResponse{Order: &pb.OrderResult{OrderId: orderID}}
}

func TestIdempotencyStoreDeduplicatesSequentialRequests(t *testing.T) {
	store := newTestIdempotencyStore(time.Minute)
	var calls atomic.Int32
	fn := func() (*pb.PlaceOrderResponse, error) {
		calls.Add(1)
		return successfulOrder("order-1"), nil
	}

	first, err := store.Do(context.Background(), "key-1", fn)
	if err != nil {
		t.Fatalf("first Do() error: %v", err)
	}
	second, err := store.Do(context.Background(), "key-1", fn)
	if err != nil {
		t.Fatalf("second Do() error: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("work executed %d times, want 1", got)
	}
	if first.GetOrder().GetOrderId() != second.GetOrder().GetOrderId() {
		t.Errorf("order IDs differ: %q vs %q", first.GetOrder().GetOrderId(), second.GetOrder().GetOrderId())
	}
}

func TestIdempotencyStoreCoalescesConcurrentRequests(t *testing.T) {
	store := newTestIdempotencyStore(time.Minute)
	var calls atomic.Int32
	var startedOnce sync.Once
	started := make(chan struct{})
	release := make(chan struct{})

	fn := func() (*pb.PlaceOrderResponse, error) {
		calls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return successfulOrder("order-1"), nil
	}

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			if _, err := store.Do(context.Background(), "shared-key", fn); err != nil {
				t.Errorf("Do() error: %v", err)
			}
		}()
	}

	<-started
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("work executed %d times, want 1", got)
	}
}

func TestIdempotencyStoreRunsEveryCallWithoutKey(t *testing.T) {
	store := newTestIdempotencyStore(time.Minute)
	var calls atomic.Int32
	fn := func() (*pb.PlaceOrderResponse, error) {
		calls.Add(1)
		return successfulOrder("order-1"), nil
	}

	for i := 0; i < 3; i++ {
		if _, err := store.Do(context.Background(), "", fn); err != nil {
			t.Fatalf("Do() error: %v", err)
		}
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("work executed %d times, want 3", got)
	}
}

func TestIdempotencyStoreExpiresAfterTTL(t *testing.T) {
	store := newTestIdempotencyStore(time.Minute)
	now := time.Date(2026, time.October, 26, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	var calls atomic.Int32
	fn := func() (*pb.PlaceOrderResponse, error) {
		calls.Add(1)
		return successfulOrder("order-1"), nil
	}

	if _, err := store.Do(context.Background(), "key-1", fn); err != nil {
		t.Fatalf("first Do() error: %v", err)
	}

	now = now.Add(30 * time.Second)
	if _, err := store.Do(context.Background(), "key-1", fn); err != nil {
		t.Fatalf("second Do() error: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("work executed %d times before TTL expiry, want 1", got)
	}

	now = now.Add(31 * time.Second)
	if _, err := store.Do(context.Background(), "key-1", fn); err != nil {
		t.Fatalf("third Do() error: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("work executed %d times after TTL expiry, want 2", got)
	}
}

func TestIdempotencyKeyFromIncomingMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		idempotencyKeyHeader, "incoming-key-1",
	))
	if got := idempotencyKeyFromIncomingMetadata(ctx); got != "incoming-key-1" {
		t.Errorf("idempotencyKeyFromIncomingMetadata() = %q, want %q", got, "incoming-key-1")
	}

	badCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		idempotencyKeyHeader, "not a valid key",
	))
	if got := idempotencyKeyFromIncomingMetadata(badCtx); got != "" {
		t.Errorf("idempotencyKeyFromIncomingMetadata() = %q, want empty", got)
	}

	if got := idempotencyKeyFromIncomingMetadata(context.Background()); got != "" {
		t.Errorf("idempotencyKeyFromIncomingMetadata() = %q, want empty", got)
	}
}

func TestOutgoingIdempotencyContext(t *testing.T) {
	ctx := withIdempotencyKey(context.Background(), "outgoing-key-1")
	out := outgoingIdempotencyContext(ctx)

	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	got := md.Get(idempotencyKeyHeader)
	if len(got) != 1 || got[0] != "outgoing-key-1" {
		t.Errorf("metadata %q = %v, want [outgoing-key-1]", idempotencyKeyHeader, got)
	}

	empty := outgoingIdempotencyContext(context.Background())
	if _, ok := metadata.FromOutgoingContext(empty); ok {
		t.Error("expected no outgoing metadata when key is absent")
	}
}
