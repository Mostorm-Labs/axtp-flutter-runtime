import 'dart:convert';

import 'package:axtp_flutter/axtp_flutter.dart';

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

bool bytesEqual(Iterable<int> left, Iterable<int> right) {
  final a = List<int>.from(left);
  final b = List<int>.from(right);
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

Future<void> main() async {
  check(
    RegistryLookup.methodIdByName('audio.getAlgorithmConfig') ==
        MethodId.audioGetAlgorithmConfig.value,
    'generated registry lookup failed',
  );

  final chunks = <Bytes>[];
  final body = List<int>.generate(40, (index) => index);
  OutboundProcessor(chunks.add, maxFrameSize: 24).sendRpcRequest(
    RpcPayload(
      encoding: rpcEncodingJsonBinary,
      op: RpcOp.request,
      requestId: 43,
      methodOrEventId: 0x0101,
      bodyEncoding: RpcBodyEncoding.tlv8,
      body: body,
    ),
  );
  final sink = CapturingPayloadSink();
  final inbound = InboundProcessor(sink);
  for (final chunk in chunks) {
    inbound.onBytes(chunk);
  }
  check(sink.rpcs.length == 1, 'framed rpc was not decoded');
  check(bytesEqual(sink.rpcs.single.body, body), 'framed rpc body mismatch');

  final broker = BasicBroker();
  final endpoint = AxtpEndpoint(broker);
  final transport = MockTransport();
  endpoint.attachTransport(transport);
  broker.registerMethod(0x0901, (_) => <int>[0x77]);

  final requestChunks = <Bytes>[];
  OutboundProcessor(requestChunks.add).sendRpcRequest(
    RpcPayload(
      encoding: rpcEncodingJsonBinary,
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
  check(outgoing != null, 'endpoint produced no response');
  final responseSink = CapturingPayloadSink();
  InboundProcessor(responseSink).onBytes(outgoing!);
  check(
      responseSink.rpcs.single.op == RpcOp.requestResponse, 'bad response op');
  check(bytesEqual(responseSink.rpcs.single.body, <int>[0x77]),
      'bad response body');

  final client = AxtpClient();
  client.registerMethod(MethodId.audioGetAlgorithmConfig.value, (request) {
    check(utf8.decode(request.body) == '{}', 'client json request mismatch');
    return utf8.encode('{"ok":true}');
  });
  final jsonResponse = await client.callJson('audio.getAlgorithmConfig', '{}');
  check(
    (jsonDecode(jsonResponse) as Map<String, Object?>)['ok'] == true,
    'client json response mismatch',
  );

  final wsBroker = BasicBroker();
  final wsEndpoint = AxtpEndpoint(wsBroker);
  final wsTransport = MockTransport(
    profile: const TransportProfile(
      kind: TransportKind.mock,
      wireMode: AxtpWireMode.webSocketJsonRpc,
      defaultRpcEncoding: RpcEncoding.json,
      messageOriented: true,
      supportsTextMessage: true,
      supportsBinaryMessage: false,
    ),
  );
  wsEndpoint.attachTransport(wsTransport);
  wsBroker.registerJsonMethod('audio.getAlgorithmConfig', (context, params) {
    check(context.methodName == 'audio.getAlgorithmConfig', 'bad method name');
    check(params == '{}', 'bad params');
    return '{"ok":true}';
  });
  wsTransport.injectIncoming(
    utf8.encode(
      '{"sid":"s1","op":7,"d":{"id":1,"method":"audio.getAlgorithmConfig","params":{}}}',
    ),
  );
  wsEndpoint.poll();
  final wsOutgoing = wsTransport.tryPopOutgoing();
  check(wsOutgoing != null, 'websocket path produced no response');
  final wsResponse =
      jsonDecode(utf8.decode(wsOutgoing!)) as Map<String, Object?>;
  check(wsResponse['op'] == RpcOp.requestResponse.value, 'bad websocket op');

  print('AXTP Flutter smoke OK');
}
