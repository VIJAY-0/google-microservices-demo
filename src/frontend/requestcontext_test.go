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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestValidRequestID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{name: "uuid", id: "123e4567-e89b-12d3-a456-426614174000", want: true},
		{name: "max length", id: strings.Repeat("a", 128), want: true},
		{name: "empty", id: "", want: false},
		{name: "too long", id: strings.Repeat("a", 129), want: false},
		{name: "space", id: "not a valid id", want: false},
		{name: "newline", id: "abc\n", want: false},
		{name: "unicode", id: "request-í", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validRequestID(tt.id); got != tt.want {
				t.Errorf("validRequestID(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestRequestIDFromMetadataSkipsInvalidValues(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		requestIDHeader, "invalid id",
		requestIDHeader, "valid-request-id",
	))
	if got := requestIDFromMetadata(ctx); got != "valid-request-id" {
		t.Errorf("requestIDFromMetadata() = %q, want %q", got, "valid-request-id")
	}
}

func TestLogHandlerReusesIncomingRequestID(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	var got string
	handler := &logHandler{log: logger, next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = requestIDFromContext(r.Context())
	})}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-Id", "incoming-request-id")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got != "incoming-request-id" {
		t.Errorf("handler context request ID = %q, want %q", got, "incoming-request-id")
	}
	if h := rec.Header().Get("X-Request-Id"); h != "incoming-request-id" {
		t.Errorf("response X-Request-Id = %q, want %q", h, "incoming-request-id")
	}
}

func TestLogHandlerMintsRequestIDWhenMissingOrInvalid(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	tests := []struct {
		name   string
		header string
	}{
		{name: "missing"},
		{name: "invalid", header: "not valid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			handler := &logHandler{log: logger, next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = requestIDFromContext(r.Context())
			})}

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				req.Header.Set("X-Request-Id", tt.header)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if !validRequestID(got) {
				t.Errorf("minted request ID %q is not valid", got)
			}
			if h := rec.Header().Get("X-Request-Id"); h != got {
				t.Errorf("response X-Request-Id = %q, want %q", h, got)
			}
		})
	}
}

func TestClientInterceptorPropagatesRequestID(t *testing.T) {
	ctx := withRequestID(context.Background(), "existing-request-id")

	var got string
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			t.Fatal("outgoing metadata missing")
		}
		values := md.Get(requestIDHeader)
		if len(values) != 1 {
			t.Fatalf("outgoing %s values = %v, want exactly one", requestIDHeader, values)
		}
		got = values[0]
		return nil
	}

	if err := requestIDUnaryClientInterceptor()(ctx, "/test/Method", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if got != "existing-request-id" {
		t.Errorf("propagated request ID = %q, want %q", got, "existing-request-id")
	}
}

func TestRequestIDPropagationOverGRPC(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	received := make(chan string, 1)
	recorder := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		received <- requestIDFromContext(ctx)
		return handler(ctx, req)
	}

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		requestIDUnaryServerInterceptor(logger),
		recorder,
	))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(listener)
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(requestIDUnaryClientInterceptor()))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	ctx := withRequestID(context.Background(), "propagated-request-id")
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	if got := <-received; got != "propagated-request-id" {
		t.Errorf("server received request ID %q, want %q", got, "propagated-request-id")
	}
}
