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

class ClientTrace {
  const ClientTrace({
    required this.stage,
    required this.direction,
    this.sid = '',
    this.detail = '',
  });

  final String stage;
  final String direction;
  final String sid;
  final String detail;
}

class AxtpClient {
  AxtpClient({ClientOptions options = const ClientOptions()})
      : _options = options {
    _endpoint = _createEndpoint();
  }

  final ClientOptions _options;
  final Map<int, RawMethodHandler> _localHandlers = <int, RawMethodHandler>{};
  late AxtpEndpoint _endpoint;
  AxtpTransport? _transport;
  MethodRegistry registry = MethodRegistry.fromGeneratedDefaults();
  int _nextRequestId = 1;
  bool _connected = false;
  bool _appReady = false;
  String _sessionSid = '';
  SdkError _lastError = SdkError.success;
  final StreamController<ClientTrace> _traceController =
      StreamController<ClientTrace>.broadcast(sync: true);
  final StreamController<RpcPayload> _eventController =
      StreamController<RpcPayload>.broadcast(sync: true);

  bool get isConnected => _connected;

  bool get isAppReady => _appReady;

  String get sessionSid => _sessionSid;

  SdkError get lastError => _lastError;

  Stream<ClientTrace> get traces => _traceController.stream;

  /// 服务端主动发送的 AXTP event 流。
  ///
  /// runtime 将 WebSocket JSON 的 `op=6` 解码成 [RpcPayload] 后，通过这个流
  /// 交给上层客户端。这里保留 runtime 的协议模型，不直接依赖 WebSocket 或
  /// Flutter 类型；产品层可以继续把它转换成更易用的业务事件模型。
  Stream<RpcPayload> get events => _eventController.stream;

  Future<void> attachTransport(AxtpTransport transport) async {
    await close();
    _transport = transport;
    _endpoint = _createEndpoint();
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
    _appReady = false;
    _sessionSid = '';
  }

  void poll() {
    _endpoint.poll();
  }

  void registerMethod(int methodId, RawMethodHandler handler) {
    _localHandlers[methodId] = handler;
  }

  AxtpEndpoint _createEndpoint() {
    return AxtpEndpoint(
      BasicBroker(),
      onRpcEvent: _onRpcEvent,
    );
  }

  void _onRpcEvent(RpcPayload event) {
    if (!_eventController.isClosed) {
      _eventController.add(event);
    }
  }

  Future<SdkError> ensureAppReady({
    Duration timeout = const Duration(seconds: 1),
    String eventMasks = '',
    int? randomSeed,
  }) async {
    final transport = _transport;
    if (transport == null || !_connected) {
      _lastError = SdkError.failure(
        ErrorCode.unavailable,
        'transport unavailable',
      );
      return _lastError;
    }
    if (transport.profile.wireMode != AxtpWireMode.webSocketJsonRpc) {
      _lastError = SdkError.failure(
        ErrorCode.notSupported,
        'APP_READY session helper currently targets WebSocket JSON RPC',
      );
      return _lastError;
    }
    if (_appReady) return SdkError.success;

    // 假设 timeout = 1s，那么整个 Hello、Identify、Identified 流程必须在这个截止时间前完成
    final deadline = DateTime.now().add(timeout);
    _emitTrace(const ClientTrace(stage: 'hello', direction: 'wait'));
    // 这里是在等待服务端下发的 hello
    final hello = await _waitForSession(RpcOp.hello, deadline);
    if (hello == null) {
      _lastError = SdkError.failure(
        ErrorCode.rpcResponseTimeout,
        'hello timeout',
      );
      _emitTrace(ClientTrace(
        stage: 'hello',
        direction: 'timeout',
        detail: _lastError.message,
      ));
      return _lastError;
    }
    _emitTrace(ClientTrace(
      stage: 'hello',
      direction: 'receive',
      detail: utf8.decode(hello.body),
    ));

    var seed =
        randomSeed ?? (DateTime.now().microsecondsSinceEpoch & 0xffffffff);
    if (seed == 0) seed = 1;
    // 收到 hello 了，创建 identify
    final identify = RpcPayload(
      op: RpcOp.identify,
      meta: const PayloadMeta(sourceProtocol: SourceProtocol.jsonRpc),
      body: utf8.encode(jsonEncode(<String, Object?>{
        'randomSeed': seed,
        'eventMasks': eventMasks,
      })),
    );
    // 发出 identify
    _endpoint.sendRpcSession(identify);
    _emitTrace(ClientTrace(
      stage: 'identify',
      direction: 'send',
      detail: utf8.decode(identify.body),
    ));

    // 跟 hello 一样，等待 identified 消息
    final identified = await _waitForSession(RpcOp.identified, deadline);
    if (identified == null || identified.meta.jsonSid.isEmpty) {
      _lastError = SdkError.failure(
        ErrorCode.rpcResponseTimeout,
        'identified timeout or empty sid',
      );
      _emitTrace(ClientTrace(
        stage: 'identified',
        direction: 'timeout',
        detail: _lastError.message,
      ));
      return _lastError;
    }

    // 保存服务器返回的 sid，后续业务 RPC 会自动使用这个 sid。
    _sessionSid = identified.meta.jsonSid;
    _appReady = true;
    _lastError = SdkError.success;
    _emitTrace(ClientTrace(
      stage: 'identified',
      direction: 'receive',
      sid: _sessionSid,
      detail: utf8.decode(identified.body),
    ));
    return SdkError.success;
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

    if (transport.profile.wireMode == AxtpWireMode.webSocketJsonRpc &&
        !_appReady) {
      _lastError = SdkError.failure(
        ErrorCode.unavailable,
        'AXTP WebSocket session is not identified',
      );
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
      meta: request.meta.copyWith(
        requestId: requestId,
        jsonSid: request.meta.jsonSid.isNotEmpty
            ? request.meta.jsonSid
            : _sessionSid,
      ),
    );
  }

  Future<RpcPayload?> _waitForSession(
    RpcOp op,
    DateTime deadline,
  ) async {
    // 只要还没有超过握手截止时间，就继续等待
    while (DateTime.now().isBefore(deadline)) {
      // 这里实际在调用 AxtpEndpoint.poll()
      // 严格说：WebSocket 收到消息的入口不是 poll()，而是 WebSocket 的消息监听回调。
      // 消息到达时：socket.messages.listen(_onMessage);
      // 会直接调用：AxtpWebSocketTransport._onMessage()
      //     -> ByteSink.onBytes()
      //     -> AxtpCore
      //     -> JsonRpcDecoder
      // 然后 Hello 会被放入：_sessionRpcs[RpcOp.hello]，这种事件不是业务事件，不需要下发到 web_socket_client，只会存在 _sessionRpcs 里
      // 然后 while 循环不断地去 _sessionRpcs 检查有没有存进去 hello 就代表有没有收到服务端的 hello
      poll();
      // 如果传入：RpcOp.hello
      // 它会尝试从：_sessionRpcs[RpcOp.hello] 中取出一条 Hello。
      // 如果没获取到，代表服务器还没发，就等待下一次循环
      final payload = _endpoint.tryTakeSessionRpc(op);
      // 如果如果已经收到，就返回这个 RpcPayload
      if (payload != null) return payload;
      // 这一步非常重要。它的作用是：暂时让出 Dart 事件循环，让 WebSocket 的异步消息回调有机会执行。
      // 如果没有这个 await，while 循环可能一直占用当前执行权，WebSocket 消息回调反而没有机会运行。
      await Future<void>.delayed(const Duration(milliseconds: 1));
    }
    // 如果服务器一直不发 Hello，循环最终会退出，返回：null
    // 最终就是给 axtp_websocket_client 哪里返回握手超时
    return null;
  }

  void _emitTrace(ClientTrace trace) {
    if (!_traceController.isClosed) _traceController.add(trace);
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
