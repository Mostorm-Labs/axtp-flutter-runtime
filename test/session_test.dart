import 'dart:convert';

import 'package:axtp_flutter/axtp_flutter.dart';
import 'package:test/test.dart';

void main() {
  const websocketProfile = TransportProfile(
    kind: TransportKind.webSocket,
    wireMode: AxtpWireMode.webSocketJsonRpc,
    defaultRpcEncoding: RpcEncoding.json,
    messageOriented: true,
    supportsTextMessage: true,
    supportsBinaryMessage: false,
    preferredFrameSize: 0,
  );

  test('queues WebSocket session RPC frames for the session layer', () {
    final core = AxtpCore()..configure(websocketProfile);

    core.byteSink.onBytes(utf8.encode(jsonEncode(<String, Object?>{
      'sid': '',
      'op': RpcOp.hello.value,
      'd': <String, Object?>{'axtpVersion': '0.13.0'},
    })));
    core.byteSink.onBytes(utf8.encode(jsonEncode(<String, Object?>{
      'sid': 'ABC12345',
      'op': RpcOp.identified.value,
      'd': <String, Object?>{},
    })));

    final hello = core.tryTakeSessionRpc(RpcOp.hello);
    final identified = core.tryTakeSessionRpc(RpcOp.identified);

    expect(hello, isNotNull);
    expect(hello!.op, RpcOp.hello);
    expect(identified, isNotNull);
    expect(identified!.meta.jsonSid, 'ABC12345');
  });

  test('encodes an AXTP Identify session message', () {
    final bytes = JsonRpcEncoder().encode(
      RpcPayload(
        op: RpcOp.identify,
        meta: const PayloadMeta(sourceProtocol: SourceProtocol.jsonRpc),
        body: utf8.encode(jsonEncode(<String, Object?>{
          'randomSeed': 17,
          'eventMasks': 'cast',
        })),
      ),
    );

    final message = jsonDecode(utf8.decode(bytes)) as Map<String, Object?>;
    expect(message['sid'], '');
    expect(message['op'], RpcOp.identify.value);
    expect(message['d'], <String, Object?>{
      'randomSeed': 17,
      'eventMasks': 'cast',
    });
  });

  test('decodes WebSocket RPC responses into the pending response queue', () {
    final core = AxtpCore()..configure(websocketProfile);
    core.expectRpcResponse(9);

    core.byteSink.onBytes(utf8.encode(jsonEncode(<String, Object?>{
      'sid': 'ABC12345',
      'op': RpcOp.requestResponse.value,
      'd': <String, Object?>{
        'id': 9,
        'status': <String, Object?>{'ok': true, 'code': 0},
        'result': <String, Object?>{'healthy': true},
      },
    })));

    final response = core.tryTakeRpcResponse(9);

    expect(response, isNotNull);
    expect(response!.statusCode, ErrorCode.success);
    expect(jsonDecode(utf8.decode(response.body)), <String, Object?>{
      'healthy': true,
    });
    expect(response.meta.jsonSid, 'ABC12345');
  });

  test('client reaches APP_READY and carries the assigned sid', () async {
    final transport = _HandshakeTransport();
    final client = AxtpClient(
      options: const ClientOptions(
        wireMode: AxtpWireMode.webSocketJsonRpc,
        defaultTimeout: Duration(milliseconds: 20),
      ),
    );

    await client.attachTransport(transport);
    final status = await client.ensureAppReady(
      timeout: const Duration(milliseconds: 50),
      eventMasks: 'cast',
      randomSeed: 17,
    );

    expect(status.ok, isTrue);
    expect(client.isAppReady, isTrue);
    expect(client.sessionSid, 'ABC12345');
    expect(transport.identifyPayload, <String, Object?>{
      'randomSeed': 17,
      'eventMasks': 'cast',
    });
  });

  test('client rejects WebSocket business RPC before APP_READY', () async {
    final transport = _HandshakeTransport(sendHello: false);
    final client = AxtpClient(
      options: const ClientOptions(
        wireMode: AxtpWireMode.webSocketJsonRpc,
        defaultTimeout: Duration(milliseconds: 20),
      ),
    );

    await client.attachTransport(transport);
    final response = await client.callJson(
      'cast.getStatus',
      '{}',
      options: const CallOptions(timeout: Duration(milliseconds: 10)),
    );

    expect(client.isAppReady, isFalse);
    expect(client.lastError.code, ErrorCode.unavailable);
    expect(response, isEmpty);
    expect(transport.businessRequests, isEmpty);
  });

  test('publishes server events without echoing them back to transport',
      () async {
    final transport = _HandshakeTransport();
    final client = AxtpClient(
      options: const ClientOptions(
        wireMode: AxtpWireMode.webSocketJsonRpc,
        defaultTimeout: Duration(milliseconds: 20),
      ),
    );
    final events = <RpcPayload>[];
    final subscription = client.events.listen(events.add);

    await client.attachTransport(transport);
    final status = await client.ensureAppReady(
      timeout: const Duration(milliseconds: 50),
      eventMasks: 'cast.*',
      randomSeed: 17,
    );
    expect(status.ok, isTrue);

    transport.injectEvent(
      sid: client.sessionSid,
      event: 'cast.sessionStarted',
      data: <String, Object?>{'sessionId': 'demo-1'},
    );
    await Future<void>.delayed(Duration.zero);

    expect(events, hasLength(1));
    expect(events.single.op, RpcOp.event);
    expect(events.single.meta.jsonSid, client.sessionSid);
    expect(events.single.meta.jsonMethodOrEventName, 'cast.sessionStarted');
    expect(jsonDecode(utf8.decode(events.single.body)), <String, Object?>{
      'sessionId': 'demo-1',
    });
    expect(
      transport.sentMessages.where(
        (message) => message['op'] == RpcOp.event.value,
      ),
      isEmpty,
    );

    await subscription.cancel();
    await client.close();
  });
}

class _HandshakeTransport implements AxtpTransport {
  _HandshakeTransport({this.sendHello = true});

  final bool sendHello;
  ByteSink? _sink;
  Map<String, Object?>? identifyPayload;
  final List<Map<String, Object?>> businessRequests = <Map<String, Object?>>[];
  final List<Map<String, Object?>> sentMessages = <Map<String, Object?>>[];

  @override
  TransportProfile get profile => const TransportProfile(
        kind: TransportKind.webSocket,
        wireMode: AxtpWireMode.webSocketJsonRpc,
        defaultRpcEncoding: RpcEncoding.json,
        messageOriented: true,
        supportsTextMessage: true,
        supportsBinaryMessage: false,
        preferredFrameSize: 0,
      );

  @override
  void bind(ByteSink sink) {
    _sink = sink;
  }

  @override
  Future<void> open() async {
    if (!sendHello) return;
    _sink!.onBytes(utf8.encode(jsonEncode(<String, Object?>{
      'sid': '',
      'op': RpcOp.hello.value,
      'd': <String, Object?>{'axtpVersion': '0.13.0'},
    })));
  }

  @override
  Future<void> close() async {}

  @override
  void sendBytes(Bytes bytes) {
    final message = jsonDecode(utf8.decode(bytes)) as Map<String, Object?>;
    sentMessages.add(message);
    final op = message['op'];
    final data = message['d'];
    if (op == RpcOp.identify.value) {
      identifyPayload = Map<String, Object?>.from(data! as Map);
      _sink!.onBytes(utf8.encode(jsonEncode(<String, Object?>{
        'sid': 'ABC12345',
        'op': RpcOp.identified.value,
        'd': <String, Object?>{},
      })));
      return;
    }
    if (op == RpcOp.request.value) {
      businessRequests.add(message);
    }
  }

  void injectEvent({
    required String sid,
    required String event,
    required Map<String, Object?> data,
  }) {
    _sink!.onBytes(utf8.encode(jsonEncode(<String, Object?>{
      'sid': sid,
      'op': RpcOp.event.value,
      'd': <String, Object?>{
        'event': event,
        'data': data,
      },
    })));
  }
}
