import 'dart:collection';

import 'broker.dart';
import 'generated/axtp_registry_generated.dart';
import 'model.dart';
import 'transport.dart';
import 'wire.dart';

class AxtpCore implements PayloadSink {
  AxtpCore()
      : _inbound = InboundProcessor(_PayloadSinkPort()),
        _outbound = OutboundProcessor((_) {}) {
    _byteSink = _ByteSinkPort(this);
    _payloadPort = _PayloadSinkPort(this);
    _inbound = InboundProcessor(_payloadPort);
    _outbound = OutboundProcessor(_enqueueOutboundBytes);
  }

  late final ByteSink _byteSink;
  late final _PayloadSinkPort _payloadPort;
  late InboundProcessor _inbound;
  late OutboundProcessor _outbound;
  final Queue<CoreEvent> _events = Queue<CoreEvent>();
  final Queue<Bytes> _outboundBytes = Queue<Bytes>();
  final Set<int> _pendingRequests = <int>{};
  final Map<int, RpcPayload> _resolvedResponses = <int, RpcPayload>{};
  bool _controlSessionOpen = false;
  ControlOpcode _lastControlOpcode = ControlOpcode.open;

  ByteSink get byteSink => _byteSink;

  bool get controlSessionOpen => _controlSessionOpen;

  ControlOpcode get lastControlOpcode => _lastControlOpcode;

  void configure(TransportProfile profile) {
    _inbound.wireMode = profile.wireMode;
    _outbound.wireMode = profile.wireMode;
    if (profile.preferredFrameSize > 0) {
      _outbound.maxFrameSize = profile.preferredFrameSize;
    }
  }

  CoreEvent? pollEvent() {
    if (_events.isEmpty) return null;
    return _events.removeFirst();
  }

  void handleBrokerResult(BrokerResult result) {
    switch (result.type) {
      case BrokerResultType.rpcResponse:
        _outbound.sendRpcResponse(result.rpc);
      case BrokerResultType.rpcError:
        _outbound.sendRpcError(result.rpc);
      case BrokerResultType.event:
        _outbound.sendEvent(result.rpc);
      case BrokerResultType.streamData:
      case BrokerResultType.streamClose:
        _outbound.sendStream(result.stream);
      case BrokerResultType.noop:
        break;
    }
  }

  void expectRpcResponse(int requestId) {
    _pendingRequests.add(requestId);
  }

  RpcPayload? tryTakeRpcResponse(int requestId) {
    return _resolvedResponses.remove(requestId);
  }

  Bytes? tryPopOutboundBytes() {
    if (_outboundBytes.isEmpty) return null;
    return _outboundBytes.removeFirst();
  }

  void sendRpcRequest(RpcPayload payload) {
    _outbound.sendRpcRequest(payload);
  }

  @override
  void onControl(ControlPayload payload) {
    final response = _handleControl(payload);
    if (response != null) {
      _outbound.sendControl(response);
    }
  }

  @override
  void onRpc(RpcPayload payload) {
    if (payload.op == RpcOp.request) {
      _events.add(CoreEvent.rpcRequest(payload));
      return;
    }
    if (payload.op == RpcOp.event) {
      _events.add(CoreEvent.rpcEvent(payload));
      return;
    }
    if (payload.op == RpcOp.requestResponse) {
      if (!_pendingRequests.contains(payload.requestId) &&
          payload.meta.sourceProtocol == SourceProtocol.jsonRpc) {
        _outbound.sendRpcResponse(payload);
        return;
      }
      _pendingRequests.remove(payload.requestId);
      _resolvedResponses[payload.requestId] = payload;
      return;
    }
    if (payload.op == RpcOp.requestBatchResponse &&
        payload.meta.sourceProtocol == SourceProtocol.jsonRpc) {
      _outbound.sendRpcResponse(payload);
    }
  }

  @override
  void onStream(StreamPayload payload) {
    _events.add(CoreEvent.streamData(payload));
  }

  void _handleBytes(Bytes bytes) {
    _inbound.onBytes(bytes);
  }

  void _enqueueOutboundBytes(Bytes bytes) {
    _outboundBytes.add(bytesFrom(bytes));
  }

  ControlPayload? _handleControl(ControlPayload payload) {
    _lastControlOpcode = payload.opcode;
    if (payload.opcode == ControlOpcode.open) {
      _controlSessionOpen = true;
      return _makeControlResponse(ControlOpcode.accept, payload);
    }
    if (payload.opcode == ControlOpcode.ping) {
      return _makeControlResponse(ControlOpcode.pong, payload);
    }
    if (payload.opcode == ControlOpcode.close) {
      _controlSessionOpen = false;
      return _makeControlResponse(ControlOpcode.closeAck, payload);
    }
    return null;
  }

  ControlPayload _makeControlResponse(
    ControlOpcode opcode,
    ControlPayload request,
  ) {
    return ControlPayload(
      opcode: opcode,
      controlId: request.controlId,
      statusCode: ErrorCode.success,
      meta: request.meta,
    );
  }
}

class _ByteSinkPort implements ByteSink {
  _ByteSinkPort(this._core);

  final AxtpCore _core;

  @override
  void onBytes(Bytes bytes) {
    _core._handleBytes(bytes);
  }
}

class _PayloadSinkPort implements PayloadSink {
  _PayloadSinkPort([this._core]);

  final AxtpCore? _core;

  @override
  void onControl(ControlPayload payload) {
    _core?.onControl(payload);
  }

  @override
  void onRpc(RpcPayload payload) {
    _core?.onRpc(payload);
  }

  @override
  void onStream(StreamPayload payload) {
    _core?.onStream(payload);
  }
}
