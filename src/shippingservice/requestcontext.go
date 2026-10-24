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
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const requestIDHeader = "x-request-id"

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type requestIDKey struct{}

func newRequestID() string {
	return uuid.NewString()
}

func validRequestID(id string) bool {
	return requestIDPattern.MatchString(id)
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func requestIDFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, id := range md.Get(requestIDHeader) {
		if validRequestID(id) {
			return id
		}
	}
	return ""
}

func requestIDUnaryServerInterceptor(logger *logrus.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		id := requestIDFromMetadata(ctx)
		if id == "" {
			id = newRequestID()
		}
		ctx = withRequestID(ctx, id)
		_ = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, id))

		start := time.Now()
		resp, err := handler(ctx, req)
		logger.WithFields(logrus.Fields{
			"request_id":  id,
			"rpc.method":  info.FullMethod,
			"rpc.status":  status.Code(err).String(),
			"duration_ms": time.Since(start).Milliseconds(),
		}).Info("rpc handled")
		return resp, err
	}
}

func requestIDUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		id := requestIDFromContext(ctx)
		if id == "" {
			id = newRequestID()
			ctx = withRequestID(ctx, id)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, requestIDHeader, id)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
