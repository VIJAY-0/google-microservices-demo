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
import uuid

import grpc

REQUEST_ID_HEADER = 'x-request-id'

_REQUEST_ID_PATTERN = re.compile(r'^[A-Za-z0-9._:-]{1,128}$')


def new_request_id():
  return str(uuid.uuid4())


def valid_request_id(value):
  return isinstance(value, str) and _REQUEST_ID_PATTERN.fullmatch(value) is not None


def request_id_from_metadata(metadata):
  for key, value in metadata or ():
    if key == REQUEST_ID_HEADER and valid_request_id(value):
      return value
  return None


def request_id_from_context(context):
  return getattr(context, 'request_id', None)


def request_id_metadata(context):
  request_id = request_id_from_context(context) or new_request_id()
  return ((REQUEST_ID_HEADER, request_id),)


class RequestContextServerInterceptor(grpc.ServerInterceptor):
  def __init__(self, logger):
    self._logger = logger

  def intercept_service(self, continuation, handler_call_details):
    handler = continuation(handler_call_details)
    if handler is None:
      return None

    request_id = (request_id_from_metadata(handler_call_details.invocation_metadata)
                  or new_request_id())
    if handler.unary_unary is None:
      return handler

    method = handler_call_details.method
    behavior = handler.unary_unary
    logger = self._logger

    def unary_unary(request, context):
      setattr(context, 'request_id', request_id)
      context.send_initial_metadata(((REQUEST_ID_HEADER, request_id),))
      started_at = time.monotonic()
      logger.info('rpc started', extra={
        'request_id': request_id,
        'rpc_method': method,
      })
      try:
        response = behavior(request, context)
      except Exception:
        logger.exception('rpc failed', extra={
          'request_id': request_id,
          'rpc_method': method,
          'duration_ms': _duration_ms(started_at),
        })
        raise
      logger.info('rpc handled', extra={
        'request_id': request_id,
        'rpc_method': method,
        'duration_ms': _duration_ms(started_at),
      })
      return response

    return grpc.unary_unary_rpc_method_handler(
      unary_unary,
      request_deserializer=handler.request_deserializer,
      response_serializer=handler.response_serializer
    )


def _duration_ms(started_at):
  return int((time.monotonic() - started_at) * 1000)
