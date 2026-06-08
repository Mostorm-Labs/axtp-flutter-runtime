import 'dart:async';
import 'dart:convert';

import 'broker.dart';
import 'endpoint.dart';
import 'generated/axtp_registry_generated.dart';
import 'model.dart';
import 'transport.dart';

class ClientOptions {
  const ClientOptions({
    this.autoOpen = true,
    this.wireMode = AxtpWireMode.framedBinary,
    this.defaultTimeout = const Duration(seconds: 1),
  });

  final bool autoOpen;
  final AxtpWireMode wireMode;
  final Duration defaultTimeout;
}

class CallOptions {
  const CallOptions({
    this.timeout,
    this.encoding = RpcEncoding.json,
  });

  final Duration? timeout;
  final RpcEncoding encoding;

  CallOptions copyWith({Duration? timeout, RpcEncoding? encoding}) {
    return CallOptions(
      timeout: timeout ?? this.timeout,
      encoding: encoding ?? this.encoding,
    );
  }
}

class SdkError {
  const SdkError({
    this.code = ErrorCode.success,
    this.message = '',
  });

  final ErrorCode code;
  final String message;

  bool get ok => code == ErrorCode.success;

  static const success = SdkError();

  factory SdkError.failure(ErrorCode code, [String message = '']) {
    return SdkError(code: code, message: message);
  }
}

class AxtpClient {
  AxtpClient({ClientOptions options = const ClientOptions()})
      : _options = options,
        _endpoint = AxtpEndpoint(BasicBroker());

  final ClientOptions _options;
  final Map<int, RawMethodHandler> _localHandlers = <int, RawMethodHandler>{};
  late AxtpEndpoint _endpoint;
  AxtpTransport? _transport;
  MethodRegistry registry = MethodRegistry.fromGeneratedDefaults();
  int _nextRequestId = 1;
  bool _connected = false;
  SdkError _lastError = SdkError.success;

  bool get isConnected => _connected;

  SdkError get lastError => _lastError;

  Future<void> attachTransport(AxtpTransport transport) async {
    await close();
    _transport = transport;
    _endpoint = AxtpEndpoint(BasicBroker());
    _endpoint.attachTransport(transport);
    if (_options.autoOpen) {
      await transport.open();
    }
    _connected = true;
  }

  Future<void> close() async {
    final transport = _transport;
    if (transport != null) {
      await transport.close();
    }
    _connected = false;
  }

  void poll() {
    _endpoint.poll();
  }

  void registerMethod(int methodId, RawMethodHandler handler) {
    _localHandlers[methodId] = handler;
  }

  Future<RpcPayload> callRaw(
    RpcPayload request, {
    CallOptions options = const CallOptions(),
  }) async {
    final normalized = _normalizeRequest(request, options);
    final local = _localHandlers[normalized.methodOrEventId];
    if (local != null) {
      return normalized.copyWith(
        op: RpcOp.requestResponse,
        statusCode: ErrorCode.success,
        body: local(normalized),
      );
    }

    final transport = _transport;
    if (transport == null) {
      return _makeErrorResponse(normalized, ErrorCode.unavailable);
    }

    _endpoint.core.configure(transport.profile);
    _endpoint.sendRpcRequest(normalized);

    final timeout = options.timeout ?? _options.defaultTimeout;
    final deadline = DateTime.now().add(timeout);
    while (DateTime.now().isBefore(deadline)) {
      poll();
      final response = _endpoint.tryTakeRpcResponse(normalized.requestId);
      if (response != null) {
        _lastError = response.statusCode == ErrorCode.success
            ? SdkError.success
            : SdkError.failure(response.statusCode);
        return response;
      }
      await Future<void>.delayed(const Duration(milliseconds: 1));
    }

    final timeoutResponse =
        _makeErrorResponse(normalized, ErrorCode.rpcResponseTimeout);
    _lastError = SdkError.failure(timeoutResponse.statusCode);
    return timeoutResponse;
  }

  Future<Bytes> callRawById(
    int methodId,
    RpcEncoding encoding,
    Iterable<int> body, {
    CallOptions options = const CallOptions(),
  }) async {
    final request = _makeDynamicRequest(methodId, encoding, body);
    final response = await callRaw(request, options: options);
    _lastError = response.statusCode == ErrorCode.success
        ? SdkError.success
        : SdkError.failure(response.statusCode);
    return response.body;
  }

  Future<String> callJson(
    String methodName,
    String paramsJson, {
    CallOptions options = const CallOptions(),
  }) async {
    final methodId = registry.findMethodId(methodName);
    if (methodId == null) {
      _lastError =
          SdkError.failure(ErrorCode.rpcMethodNotFound, 'method not found');
      return '';
    }
    return callJsonById(
      methodId,
      paramsJson,
      options: options.copyWith(encoding: RpcEncoding.json),
    );
  }

  Future<String> callJsonById(
    int methodId,
    String paramsJson, {
    CallOptions options = const CallOptions(),
  }) async {
    final response = await callRawById(
      methodId,
      RpcEncoding.json,
      utf8.encode(paramsJson),
      options: options.copyWith(encoding: RpcEncoding.json),
    );
    return utf8.decode(response);
  }

  Future<Bytes> callTlv(
    String methodName,
    Iterable<int> tlvBody, {
    CallOptions options = const CallOptions(),
  }) async {
    final methodId = registry.findMethodId(methodName);
    if (methodId == null) {
      _lastError =
          SdkError.failure(ErrorCode.rpcMethodNotFound, 'method not found');
      return bytesFrom(null);
    }
    return callTlvById(
      methodId,
      tlvBody,
      options: options.copyWith(encoding: rpcEncodingJsonBinary),
    );
  }

  Future<Bytes> callTlvById(
    int methodId,
    Iterable<int> tlvBody, {
    CallOptions options = const CallOptions(),
  }) {
    return callRawById(
      methodId,
      rpcEncodingJsonBinary,
      tlvBody,
      options: options.copyWith(encoding: rpcEncodingJsonBinary),
    );
  }

  Future<Bytes> callRawBytes(
    int methodId,
    Iterable<int> body, {
    CallOptions options = const CallOptions(),
  }) {
    return callRawById(
      methodId,
      rpcEncodingJsonBinary,
      body,
      options: options.copyWith(encoding: rpcEncodingJsonBinary),
    );
  }

  RpcPayload _makeDynamicRequest(
    int methodId,
    RpcEncoding encoding,
    Iterable<int> body,
  ) {
    final methodName = registry.findMethodName(methodId) ?? '';
    return RpcPayload(
      encoding: encoding,
      op: RpcOp.request,
      methodOrEventId: methodId,
      bodyEncoding: _bodyEncodingFor(encoding),
      meta: PayloadMeta(
        sourceProtocol: _options.wireMode == AxtpWireMode.webSocketJsonRpc
            ? SourceProtocol.jsonRpc
            : SourceProtocol.axtpV1,
        jsonMethodOrEventName: methodName,
      ),
      body: body,
    );
  }

  RpcPayload _normalizeRequest(RpcPayload request, CallOptions options) {
    final requestId =
        request.requestId == 0 ? _takeRequestId() : request.requestId;
    var bodyEncoding = request.bodyEncoding;
    if (bodyEncoding == RpcBodyEncoding.tlv8 &&
        !isJsonBinaryRpcEncoding(request.encoding)) {
      bodyEncoding = _bodyEncodingFor(request.encoding);
    }
    return request.copyWith(
      op: RpcOp.request,
      requestId: requestId,
      bodyEncoding: bodyEncoding,
      meta: request.meta.copyWith(requestId: requestId),
    );
  }

  int _takeRequestId() {
    final id = _nextRequestId;
    _nextRequestId = (_nextRequestId + 1) & 0xffffffff;
    if (_nextRequestId == 0) _nextRequestId = 1;
    return id;
  }

  RpcBodyEncoding _bodyEncodingFor(RpcEncoding encoding) {
    return bodyEncodingForRpcEncoding(encoding);
  }

  RpcPayload _makeErrorResponse(RpcPayload request, ErrorCode code) {
    return request.copyWith(
      op: RpcOp.requestResponse,
      statusCode: code,
      body: const <int>[],
    );
  }
}
