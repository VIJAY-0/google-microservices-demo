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

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;

namespace cartservice
{
    public static class RequestContext
    {
        public const string HeaderName = "x-request-id";

        private static readonly AsyncLocal<string> CurrentRequestId = new();

        public static string RequestId
        {
            get => CurrentRequestId.Value;
            set => CurrentRequestId.Value = value;
        }

        public static bool IsValid(string requestId)
        {
            if (string.IsNullOrEmpty(requestId) || requestId.Length > 128)
            {
                return false;
            }

            foreach (char c in requestId)
            {
                if (!char.IsAsciiLetterOrDigit(c) && c != '.' && c != '_' && c != ':' && c != '-')
                {
                    return false;
                }
            }

            return true;
        }

        public static string NewRequestId()
        {
            return Guid.NewGuid().ToString();
        }
    }

    public class RequestContextInterceptor : Interceptor
    {
        private readonly ILogger<RequestContextInterceptor> _logger;

        public RequestContextInterceptor(ILogger<RequestContextInterceptor> logger)
        {
            _logger = logger;
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
            TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            string incoming = context.RequestHeaders.GetValue(RequestContext.HeaderName);
            string requestId = RequestContext.IsValid(incoming)
                ? incoming
                : RequestContext.NewRequestId();

            context.ResponseHeaders.Add(RequestContext.HeaderName, requestId);
            RequestContext.RequestId = requestId;

            var stopwatch = Stopwatch.StartNew();
            _logger.LogInformation(
                "RPC {Method} started (request_id={RequestId})",
                context.Method,
                requestId);

            try
            {
                TResponse response = await continuation(request, context);
                _logger.LogInformation(
                    "RPC {Method} handled in {DurationMs}ms (request_id={RequestId})",
                    context.Method,
                    stopwatch.ElapsedMilliseconds,
                    requestId);
                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "RPC {Method} failed in {DurationMs}ms (request_id={RequestId})",
                    context.Method,
                    stopwatch.ElapsedMilliseconds,
                    requestId);
                throw;
            }
            finally
            {
                RequestContext.RequestId = null;
            }
        }
    }
}
