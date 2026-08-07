import 'broker.dart';
import 'core.dart';
import 'generated/axtp_registry_generated.dart';
import 'model.dart';
import 'transport.dart';

class AxtpEndpoint {
  AxtpEndpoint(
    this.broker, {
    this.onRpcEvent,
  }) : _byteSink = _EndpointByteSink();

  final BasicBroker broker;

  /// 接收服务端主动 RPC event 的回调。
  ///
  /// 当该回调存在时，`RpcOp.event` 不会交给默认 broker 回写，而是直接交给
  /// 上层客户端；未设置回调时保留原有 broker 行为，兼容 endpoint 的协议测试。
  final void Function(RpcPayload event)? onRpcEvent;

  late final AxtpCore core = AxtpCore(onRpcEvent: onRpcEvent);
  final _EndpointByteSink _byteSink;
  AxtpTransport? _transport;

  void attachTransport(AxtpTransport transport) {
    _transport = transport;
    core.configure(transport.profile);
    _byteSink.endpoint = this;
    transport.bind(_byteSink);
  }

  void detachTransport() {
    _transport = null;
    _byteSink.endpoint = null;
  }

  void poll([int maxTasks = 8]) {
    // 把 AxtpCore 中仍需 broker 处理的服务器主动 request、流数据等事件取出。
    // AxtpClient 配置 onRpcEvent 后，op=6 event 已在 AxtpCore.onRpc 中实时回调，
    // 不会进入这里，也不会被 broker 当成任务重新发送给服务端。
    _drainCoreEvents();
    // 让 broker 处理这些事件。
    broker.poll(maxTasks);
    // 拿到 broker 的处理结果，并交给 AxtpCore 编码。
    _drainBrokerResults();
    // 把 runtime 生成的待发送字节交给 transport
    flushOutbound();
  }

  void onTransportBytes(Bytes bytes) {
    core.byteSink.onBytes(bytes);
  }

  void sendRpcRequest(RpcPayload payload) {
    core.expectRpcResponse(payload.requestId);
    core.sendRpcRequest(payload);
    flushOutbound();
  }

  RpcPayload? tryTakeRpcResponse(int requestId) {
    return core.tryTakeRpcResponse(requestId);
  }

  RpcPayload? tryTakeSessionRpc(RpcOp op) {
    return core.tryTakeSessionRpc(op);
  }

  void sendRpcSession(RpcPayload payload) {
    core.sendRpcSession(payload);
    flushOutbound();
  }

  void flushOutbound() {
    final transport = _transport;
    if (transport == null) return;
    while (true) {
      final bytes = core.tryPopOutboundBytes();
      if (bytes == null) return;
      transport.sendBytes(bytes);
    }
  }

  void _drainCoreEvents() {
    while (true) {
      final event = core.pollEvent();
      if (event == null) return;

      broker.submit(BrokerTask.fromCoreEvent(event));
    }
  }

  void _drainBrokerResults() {
    while (true) {
      final result = broker.pollResult();
      if (result == null) return;
      core.handleBrokerResult(result);
    }
  }
}

class _EndpointByteSink implements ByteSink {
  AxtpEndpoint? endpoint;

  @override
  void onBytes(Bytes bytes) {
    endpoint?.onTransportBytes(bytes);
  }
}
