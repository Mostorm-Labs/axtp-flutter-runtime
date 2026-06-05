import 'dart:convert';

import 'package:axtp_flutter/axtp_flutter.dart';
import 'package:test/test.dart';

bool bytesEqual(Iterable<int> left, Iterable<int> right) {
  final a = List<int>.from(left);
  final b = List<int>.from(right);
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

void main() {
  test('generated registry lookup mirrors protocol facts', () {
    final methodId = RegistryLookup.methodIdByName('audio.getAlgorithmConfig');
    expect(methodId, MethodId.audioGetAlgorithmConfig.value);

    final registry = MethodRegistry.fromGeneratedDefaults()
      ..addMethod(0x90010001, 'vendor.echo');
    expect(registry.containsMethod('vendor.echo'), isTrue);
    expect(registry.findMethodName(0x90010001), 'vendor.echo');
  });

  test('framed binary outbound and inbound roundtrip fragmented rpc', () {
    final chunks = <Bytes>[];
    final outbound = OutboundProcessor(chunks.add, maxFrameSize: 24);
    final body = List<int>.generate(40, (index) => index);
    outbound.sendRpcRequest(
      RpcPayload(
        encoding: RpcEncoding.tlv,
        op: RpcOp.request,
        requestId: 43,
        methodOrEventId: 0x0101,
        bodyEncoding: RpcBodyEncoding.rawBytes,
        body: body,
      ),
    );

    final sink = CapturingPayloadSink();
    final inbound = InboundProcessor(sink);
    for (final chunk in chunks) {
      inbound.onBytes(chunk);
    }

    expect(sink.rpcs, hasLength(1));
    expect(sink.rpcs.single.requestId, 43);
    expect(sink.rpcs.single.methodOrEventId, 0x0101);
    expect(bytesEqual(sink.rpcs.single.body, body), isTrue);
  });

  test('endpoint dispatches broker handler through mock transport', () {
    final broker = BasicBroker();
    final endpoint = AxtpEndpoint(broker);
    final transport = MockTransport();
    endpoint.attachTransport(transport);

    broker.registerMethod(0x0901, (request) => <int>[0x77]);

    final requestChunks = <Bytes>[];
    OutboundProcessor(requestChunks.add).sendRpcRequest(
      RpcPayload(
        encoding: RpcEncoding.tlv,
        op: RpcOp.request,
        requestId: 900,
        methodOrEventId: 0x0901,
        bodyEncoding: RpcBodyEncoding.tlv8,
      ),
    );

    for (final chunk in requestChunks) {
      transport.injectIncoming(chunk);
    }
    endpoint.poll();

    final outgoing = transport.tryPopOutgoing();
    expect(outgoing, isNotNull);

    final sink = CapturingPayloadSink();
    InboundProcessor(sink).onBytes(outgoing!);
    expect(sink.rpcs, hasLength(1));
    expect(sink.rpcs.single.op, RpcOp.requestResponse);
    expect(sink.rpcs.single.requestId, 900);
    expect(bytesEqual(sink.rpcs.single.body, <int>[0x77]), isTrue);
  });

  test('client dynamic json and raw calls can use local handlers', () async {
    final client = AxtpClient();
    client.registerMethod(MethodId.audioGetAlgorithmConfig.value, (request) {
      expect(utf8.decode(request.body), '{}');
      return utf8.encode('{"ok":true}');
    });
    client.registry.addMethod(0x90010001, 'vendor.echo');
    client.registerMethod(0x90010001, (request) => request.body);

    final jsonResponse =
        await client.callJson('audio.getAlgorithmConfig', '{}');
    expect(jsonDecode(jsonResponse), <String, Object?>{'ok': true});

    final rawResponse =
        await client.callRawBytes(0x90010001, <int>[0xca, 0xfe]);
    expect(bytesEqual(rawResponse, <int>[0xca, 0xfe]), isTrue);
  });

  test('websocket json rpc text request dispatches json handler', () {
    final broker = BasicBroker();
    final endpoint = AxtpEndpoint(broker);
    final transport = MockTransport(
      profile: const TransportProfile(
        kind: TransportKind.mock,
        wireMode: AxtpWireMode.webSocketJsonRpc,
        defaultRpcEncoding: RpcEncoding.json,
        messageOriented: true,
        supportsTextMessage: true,
        supportsBinaryMessage: false,
      ),
    );
    endpoint.attachTransport(transport);
    broker.registerJsonMethod('audio.getAlgorithmConfig', (context, params) {
      expect(context.methodName, 'audio.getAlgorithmConfig');
      expect(params, '{}');
      return '{"ok":true}';
    });

    transport.injectIncoming(
      utf8.encode(
        '{"sid":"s1","op":7,"d":{"id":1,"method":"audio.getAlgorithmConfig","params":{}}}',
      ),
    );
    endpoint.poll();

    final outgoing = transport.tryPopOutgoing();
    expect(outgoing, isNotNull);
    final response = jsonDecode(utf8.decode(outgoing!)) as Map<String, Object?>;
    expect(response['sid'], 's1');
    expect(response['op'], RpcOp.requestResponse.value);
    final data = response['d'] as Map<String, Object?>;
    expect(data['status'], <String, Object?>{'ok': true, 'code': 0});
    expect(data['result'], <String, Object?>{'ok': true});
  });
}
