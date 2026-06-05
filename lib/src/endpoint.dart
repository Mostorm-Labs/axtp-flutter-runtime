import 'broker.dart';
import 'core.dart';
import 'model.dart';
import 'transport.dart';

class AxtpEndpoint {
  AxtpEndpoint(this.broker) : _byteSink = _EndpointByteSink();

  final BasicBroker broker;
  final AxtpCore core = AxtpCore();
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
    _drainCoreEvents();
    broker.poll(maxTasks);
    _drainBrokerResults();
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
