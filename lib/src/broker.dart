import 'dart:collection';
import 'dart:convert';

import 'generated/axtp_registry_generated.dart';
import 'model.dart';

enum BrokerTaskType {
  rpcRequest,
  rpcEvent,
  streamOpen,
  streamData,
  streamClose,
  controlNotice,
}

enum BrokerResultType {
  rpcResponse,
  rpcError,
  event,
  streamData,
  streamClose,
  noop,
}

class BrokerContext {
  const BrokerContext({
    this.sessionId = 0,
    this.requestId = 0,
    this.methodOrEventId = 0,
    this.encoding = RpcEncoding.json,
    this.sourceProtocol = SourceProtocol.axtpV1,
  });

  final int sessionId;
  final int requestId;
  final int methodOrEventId;
  final RpcEncoding encoding;
  final SourceProtocol sourceProtocol;
}

class BrokerTask {
  BrokerTask({
    this.type = BrokerTaskType.rpcRequest,
    this.context = const BrokerContext(),
    RpcPayload? rpc,
    StreamPayload? stream,
    ControlPayload? control,
  })  : rpc = rpc ?? RpcPayload(),
        stream = stream ?? StreamPayload(),
        control = control ?? ControlPayload();

  factory BrokerTask.fromCoreEvent(CoreEvent event) {
    switch (event.type) {
      case CoreEventType.rpcRequest:
        return BrokerTask(
          type: BrokerTaskType.rpcRequest,
          rpc: event.rpc,
          context: BrokerContext(
            sessionId: event.rpc.meta.sessionId,
            requestId: event.rpc.requestId,
            methodOrEventId: event.rpc.methodOrEventId,
            encoding: event.rpc.encoding,
            sourceProtocol: event.rpc.meta.sourceProtocol,
          ),
        );
      case CoreEventType.rpcEvent:
        return BrokerTask(
          type: BrokerTaskType.rpcEvent,
          rpc: event.rpc,
          context: BrokerContext(
            sessionId: event.rpc.meta.sessionId,
            methodOrEventId: event.rpc.methodOrEventId,
            encoding: event.rpc.encoding,
            sourceProtocol: event.rpc.meta.sourceProtocol,
          ),
        );
      case CoreEventType.streamOpen:
        return BrokerTask(
          type: BrokerTaskType.streamOpen,
          stream: event.stream,
          context: BrokerContext(sessionId: event.stream.meta.sessionId),
        );
      case CoreEventType.streamData:
        return BrokerTask(
          type: BrokerTaskType.streamData,
          stream: event.stream,
          context: BrokerContext(sessionId: event.stream.meta.sessionId),
        );
      case CoreEventType.streamClose:
        return BrokerTask(
          type: BrokerTaskType.streamClose,
          stream: event.stream,
          context: BrokerContext(sessionId: event.stream.meta.sessionId),
        );
      case CoreEventType.controlNotice:
        return BrokerTask(
          type: BrokerTaskType.controlNotice,
          control: event.control,
          context: BrokerContext(sessionId: event.control.meta.sessionId),
        );
      case CoreEventType.protocolError:
        return BrokerTask(type: BrokerTaskType.controlNotice);
    }
  }

  final BrokerTaskType type;
  final BrokerContext context;
  final RpcPayload rpc;
  final StreamPayload stream;
  final ControlPayload control;
}

class BrokerResult {
  BrokerResult({
    this.type = BrokerResultType.noop,
    RpcPayload? rpc,
    StreamPayload? stream,
    ControlPayload? control,
  })  : rpc = rpc ?? RpcPayload(),
        stream = stream ?? StreamPayload(),
        control = control ?? ControlPayload();

  factory BrokerResult.rpcResponse(RpcPayload payload) =>
      BrokerResult(type: BrokerResultType.rpcResponse, rpc: payload);

  factory BrokerResult.rpcError(RpcPayload payload) =>
      BrokerResult(type: BrokerResultType.rpcError, rpc: payload);

  factory BrokerResult.event(RpcPayload payload) =>
      BrokerResult(type: BrokerResultType.event, rpc: payload);

  factory BrokerResult.streamData(StreamPayload payload) =>
      BrokerResult(type: BrokerResultType.streamData, stream: payload);

  factory BrokerResult.streamClose(StreamPayload payload) =>
      BrokerResult(type: BrokerResultType.streamClose, stream: payload);

  factory BrokerResult.noop() => BrokerResult();

  final BrokerResultType type;
  final RpcPayload rpc;
  final StreamPayload stream;
  final ControlPayload control;
}

class RpcContext {
  const RpcContext({
    this.sessionId = 0,
    this.requestId = 0,
    this.methodId = 0,
    this.methodName = '',
    this.encoding = RpcEncoding.json,
    this.sourceProtocol = SourceProtocol.axtpV1,
  });

  final int sessionId;
  final int requestId;
  final int methodId;
  final String methodName;
  final RpcEncoding encoding;
  final SourceProtocol sourceProtocol;
}

class RpcRequestView {
  const RpcRequestView({
    this.methodId = 0,
    this.methodName = '',
    this.requestId = 0,
    this.encoding = RpcEncoding.json,
    required this.body,
  });

  final int methodId;
  final String methodName;
  final int requestId;
  final RpcEncoding encoding;
  final Bytes body;
}

class RpcResponseData {
  RpcResponseData({
    this.encoding = RpcEncoding.json,
    Iterable<int>? body,
    this.overrideEncoding = false,
    this.statusCode = ErrorCode.success,
    this.overrideStatus = false,
  }) : body = bytesFrom(body);

  final RpcEncoding encoding;
  final Bytes body;
  final bool overrideEncoding;
  final ErrorCode statusCode;
  final bool overrideStatus;
}

typedef RawMethodHandler = Iterable<int> Function(RpcPayload request);
typedef RawRpcHandler = RpcResponseData Function(
  RpcContext context,
  RpcRequestView request,
);
typedef JsonRpcHandler = String Function(RpcContext context, String paramsJson);
typedef TlvRpcHandler = Iterable<int> Function(
  RpcContext context,
  Bytes body,
);
typedef StreamHandler = BrokerResult? Function(
  BrokerContext context,
  StreamPayload stream,
);

class BusinessRouter {
  final MethodRegistry registry = MethodRegistry.fromGeneratedDefaults();
  final Map<int, RawRpcHandler> _handlers = <int, RawRpcHandler>{};

  void registerMethod(int methodId, RawMethodHandler handler) {
    registerRawMethod(methodId, (context, request) {
      final payload = RpcPayload(
        encoding: request.encoding,
        op: RpcOp.request,
        requestId: request.requestId,
        methodOrEventId: request.methodId,
        meta: PayloadMeta(
          sourceProtocol: context.sourceProtocol,
          sessionId: context.sessionId,
          requestId: context.requestId,
          jsonMethodOrEventName: context.methodName,
        ),
        body: request.body,
      );
      return RpcResponseData(body: handler(payload));
    });
  }

  void registerRawMethod(int methodId, RawRpcHandler handler) {
    _handlers[methodId] = handler;
  }

  void registerJsonMethodById(int methodId, JsonRpcHandler handler) {
    registerRawMethod(methodId, (context, request) {
      final result = handler(context, utf8.decode(request.body));
      return RpcResponseData(
        encoding: RpcEncoding.json,
        body: utf8.encode(result),
        overrideEncoding: true,
      );
    });
  }

  void registerJsonMethod(String methodName, JsonRpcHandler handler) {
    final methodId = registry.findMethodId(methodName);
    if (methodId == null) return;
    registerJsonMethodById(methodId, handler);
  }

  void registerTlvMethodById(int methodId, TlvRpcHandler handler) {
    registerRawMethod(methodId, (context, request) {
      return RpcResponseData(
        encoding: rpcEncodingJsonBinary,
        body: handler(context, request.body),
        overrideEncoding: true,
      );
    });
  }

  void registerTlvMethod(String methodName, TlvRpcHandler handler) {
    final methodId = registry.findMethodId(methodName);
    if (methodId == null) return;
    registerTlvMethodById(methodId, handler);
  }

  RpcPayload handleRpcRequest(RpcPayload request) {
    var response = request.copyWith(
      op: RpcOp.requestResponse,
      statusCode: ErrorCode.success,
      body: const <int>[],
    );
    final handler = _handlers[request.methodOrEventId];
    if (handler == null) {
      return response.copyWith(statusCode: ErrorCode.rpcMethodNotFound);
    }

    final methodName = registry.findMethodName(request.methodOrEventId) ?? '';
    final context = RpcContext(
      sessionId: request.meta.sessionId,
      requestId: request.requestId,
      methodId: request.methodOrEventId,
      methodName: methodName,
      encoding: request.encoding,
      sourceProtocol: request.meta.sourceProtocol,
    );
    final view = RpcRequestView(
      methodId: request.methodOrEventId,
      methodName: methodName,
      requestId: request.requestId,
      encoding: request.encoding,
      body: request.body,
    );
    final data = handler(context, view);
    if (data.overrideEncoding) {
      response = response.copyWith(
        encoding: data.encoding,
        bodyEncoding: _bodyEncodingFor(data.encoding),
      );
    }
    if (data.overrideStatus) {
      response = response.copyWith(statusCode: data.statusCode);
    }
    return response.copyWith(body: data.body);
  }

  RpcBodyEncoding _bodyEncodingFor(RpcEncoding encoding) {
    return bodyEncodingForRpcEncoding(encoding);
  }
}

class BasicBroker {
  final Queue<BrokerTask> _tasks = Queue<BrokerTask>();
  final Queue<BrokerResult> _results = Queue<BrokerResult>();
  final BusinessRouter _router = BusinessRouter();
  StreamHandler? _streamHandler;

  MethodRegistry get registry => _router.registry;

  int get queuedTaskCount => _tasks.length;

  int get queuedResultCount => _results.length;

  void submit(BrokerTask task) {
    _tasks.add(task);
  }

  void poll([int maxTasks = 8]) {
    var processed = 0;
    while (_tasks.isNotEmpty && processed < maxTasks) {
      final task = _tasks.removeFirst();
      processed += 1;
      switch (task.type) {
        case BrokerTaskType.rpcRequest:
          final response = _router.handleRpcRequest(task.rpc);
          _results.add(
            response.statusCode == ErrorCode.success
                ? BrokerResult.rpcResponse(response)
                : BrokerResult.rpcError(response),
          );
        case BrokerTaskType.rpcEvent:
          _results.add(BrokerResult.event(task.rpc));
        case BrokerTaskType.streamData:
          final handler = _streamHandler;
          if (handler == null) {
            _results.add(BrokerResult.streamData(task.stream));
          } else {
            final result = handler(task.context, task.stream);
            if (result != null) {
              _results.add(result);
            }
          }
        case BrokerTaskType.streamClose:
          _results.add(BrokerResult.streamClose(task.stream));
        case BrokerTaskType.streamOpen:
        case BrokerTaskType.controlNotice:
          break;
      }
    }
  }

  BrokerResult? pollResult() {
    if (_results.isEmpty) return null;
    return _results.removeFirst();
  }

  void registerMethod(int methodId, RawMethodHandler handler) {
    _router.registerMethod(methodId, handler);
  }

  void registerRawMethod(int methodId, RawRpcHandler handler) {
    _router.registerRawMethod(methodId, handler);
  }

  void registerJsonMethodById(int methodId, JsonRpcHandler handler) {
    _router.registerJsonMethodById(methodId, handler);
  }

  void registerJsonMethod(String methodName, JsonRpcHandler handler) {
    _router.registerJsonMethod(methodName, handler);
  }

  void registerTlvMethodById(int methodId, TlvRpcHandler handler) {
    _router.registerTlvMethodById(methodId, handler);
  }

  void registerTlvMethod(String methodName, TlvRpcHandler handler) {
    _router.registerTlvMethod(methodName, handler);
  }

  void registerStreamHandler(StreamHandler handler) {
    _streamHandler = handler;
  }
}
