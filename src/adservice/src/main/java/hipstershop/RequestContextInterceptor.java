/*
 * Copyright 2026, Google LLC.
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

package hipstershop;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Extracts or generates a request ID for every RPC, exposes it to handlers through a gRPC
 * {@link Context}, echoes it on the response headers, and logs RPC completion with its duration.
 */
public final class RequestContextInterceptor implements ServerInterceptor {

  private static final Logger logger = LogManager.getLogger(RequestContextInterceptor.class);

  private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("^[A-Za-z0-9._:-]{1,128}$");

  private static final Metadata.Key<String> REQUEST_ID_METADATA_KEY =
      Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER);

  private static final Context.Key<String> REQUEST_ID_CONTEXT_KEY = Context.key("request-id");

  public static String getRequestId() {
    return REQUEST_ID_CONTEXT_KEY.get();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    String incoming = headers.get(REQUEST_ID_METADATA_KEY);
    String requestId = isValid(incoming) ? incoming : UUID.randomUUID().toString();
    String method = call.getMethodDescriptor().getFullMethodName();
    long startedAt = System.nanoTime();

    Context context = Context.current().withValue(REQUEST_ID_CONTEXT_KEY, requestId);
    logger.info("rpc started method={} request_id={}", method, requestId);

    ServerCall<ReqT, RespT> wrappedCall =
        new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void sendHeaders(Metadata responseHeaders) {
            responseHeaders.put(REQUEST_ID_METADATA_KEY, requestId);
            super.sendHeaders(responseHeaders);
          }

          @Override
          public void close(Status status, Metadata trailers) {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            if (status.isOk()) {
              logger.info(
                  "rpc handled method={} status={} duration_ms={} request_id={}",
                  method, status.getCode(), durationMs, requestId);
            } else {
              logger.warn(
                  "rpc failed method={} status={} duration_ms={} request_id={}",
                  method, status.getCode(), durationMs, requestId);
            }
            super.close(status, trailers);
          }
        };

    return Contexts.interceptCall(context, wrappedCall, headers, next);
  }

  private static boolean isValid(String requestId) {
    return requestId != null && REQUEST_ID_PATTERN.matcher(requestId).matches();
  }
}
