import 'dart:collection';

import '../model.dart';
import '../transport.dart';

class MockTransport implements AxtpTransport {
  MockTransport({
    this.profile = const TransportProfile(kind: TransportKind.mock),
  });

  @override
  final TransportProfile profile;

  final Queue<Bytes> _outgoing = Queue<Bytes>();
  ByteSink? _sink;
  bool _open = false;

  bool get isOpen => _open;

  @override
  void bind(ByteSink sink) {
    _sink = sink;
  }

  @override
  void open() {
    _open = true;
  }

  @override
  void close() {
    _open = false;
  }

  void injectIncoming(Iterable<int> bytes) {
    final sink = _sink;
    if (sink != null) {
      sink.onBytes(bytesFrom(bytes));
    }
  }

  @override
  void sendBytes(Bytes bytes) {
    _outgoing.add(bytesFrom(bytes));
  }

  Bytes? tryPopOutgoing() {
    if (_outgoing.isEmpty) return null;
    return _outgoing.removeFirst();
  }
}
