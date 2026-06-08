import 'dart:convert';
import 'dart:io';

import 'package:axtp_flutter/axtp_flutter.dart';
import 'package:axtp_flutter/src/generated/axtp_generated_version.dart';
import 'package:test/test.dart';

enum Requirement { required, optional, notSelected, unsupported }

enum CaseStatus { pending, passed, failed, skipped, unsupported }

class CaseResult {
  CaseResult(
    this.id,
    this.level,
    this.requirement,
    this.status, [
    this.message = '',
  ]);

  final String id;
  final String level;
  final Requirement requirement;
  CaseStatus status;
  double durationMs = 0;
  String message;

  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'level': level,
        'requirement': requirement.name == 'notSelected'
            ? 'not-selected'
            : requirement.name,
        'status': status == CaseStatus.pending ? 'failed' : status.name,
        'durationMs': durationMs,
        'message': message,
      };
}

final cases = <CaseResult>[
  CaseResult('handshake.open_accept', 'framed-binary', Requirement.required,
      CaseStatus.pending),
  CaseResult(
      'handshake.open_reject',
      'framed-binary',
      Requirement.notSelected,
      CaseStatus.skipped,
      'control open rejection policy is not part of the v1 framed-binary required set'),
  CaseResult('handshake.close', 'framed-binary', Requirement.required,
      CaseStatus.pending),
  CaseResult('handshake.ping_pong', 'framed-binary', Requirement.required,
      CaseStatus.pending),
  CaseResult(
      'session.hello_identify_identified',
      'websocket-jsonrpc',
      Requirement.optional,
      CaseStatus.skipped,
      'Dart runtime exposes JSON-RPC wire encoding but no WebSocket session adapter'),
  CaseResult(
      'session.request_before_identified',
      'websocket-jsonrpc',
      Requirement.optional,
      CaseStatus.skipped,
      'Dart runtime exposes JSON-RPC wire encoding but no WebSocket session adapter'),
  CaseResult('rpc.request_response_json', 'core', Requirement.required,
      CaseStatus.pending),
  CaseResult(
      'rpc.method_not_found', 'core', Requirement.required, CaseStatus.pending),
  CaseResult(
      'rpc.invalid_params',
      'core',
      Requirement.notSelected,
      CaseStatus.skipped,
      'schema-aware parameter validation is outside the required Dart core profile'),
  CaseResult(
      'rpc.request_id_match', 'core', Requirement.required, CaseStatus.pending),
  CaseResult(
      'event.subscribe_event',
      'event',
      Requirement.optional,
      CaseStatus.skipped,
      'event subscription intent requires a WebSocket session adapter'),
  CaseResult(
      'event.unsubscribe_event',
      'event',
      Requirement.optional,
      CaseStatus.skipped,
      'event subscription intent requires a WebSocket session adapter'),
  CaseResult(
      'event.emit_event', 'event', Requirement.optional, CaseStatus.pending),
  CaseResult('capability.get_all', 'capability', Requirement.optional,
      CaseStatus.pending),
  CaseResult('capability.method_binding', 'capability', Requirement.optional,
      CaseStatus.pending),
  CaseResult('capability.unsupported_method', 'capability',
      Requirement.optional, CaseStatus.pending),
  CaseResult('error.standard_error_shape', 'core', Requirement.required,
      CaseStatus.pending),
  CaseResult(
      'error.unauthorized',
      'core',
      Requirement.notSelected,
      CaseStatus.skipped,
      'auth policy hooks are outside the required Dart core profile'),
  CaseResult(
      'error.server_busy',
      'core',
      Requirement.notSelected,
      CaseStatus.skipped,
      'busy-state policy hooks are outside the required Dart core profile'),
  CaseResult(
      'stream.stream_open',
      'stream',
      Requirement.optional,
      CaseStatus.skipped,
      'stream.open RPC control-plane method is not part of the generated spec/v0.0.2 registry'),
  CaseResult(
      'stream.stream_data', 'stream', Requirement.optional, CaseStatus.pending),
  CaseResult(
      'stream.stream_close',
      'stream',
      Requirement.optional,
      CaseStatus.skipped,
      'stream.close RPC control-plane method is not part of the generated spec/v0.0.2 registry'),
];

bool bytesEqual(Iterable<int> left, Iterable<int> right) {
  final a = List<int>.from(left);
  final b = List<int>.from(right);
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

void runCase(String id, bool Function() fn) {
  final item = cases.singleWhere((candidate) => candidate.id == id);
  final stopwatch = Stopwatch()..start();
  try {
    final ok = fn();
    item.status = ok ? CaseStatus.passed : CaseStatus.failed;
    if (!ok && item.message.isEmpty) {
      item.message = 'case returned false';
    }
  } catch (error) {
    item.status = CaseStatus.failed;
    item.message = error.toString();
  } finally {
    stopwatch.stop();
    item.durationMs = stopwatch.elapsedMicroseconds / 1000;
  }
}

List<RpcPayload> roundTripRequest(
  RpcPayload request, {
  void Function(BasicBroker broker)? configureBroker,
}) {
  final broker = BasicBroker();
  configureBroker?.call(broker);
  final endpoint = AxtpEndpoint(broker);
  final transport = MockTransport();
  endpoint.attachTransport(transport);

  final chunks = <Bytes>[];
  OutboundProcessor(chunks.add).sendRpcRequest(request);
  for (final chunk in chunks) {
    transport.injectIncoming(chunk);
  }
  endpoint.poll();

  final outgoing = transport.tryPopOutgoing();
  if (outgoing == null) {
    throw StateError('endpoint did not emit an RPC response');
  }
  final sink = CapturingPayloadSink();
  InboundProcessor(sink).onBytes(outgoing);
  return sink.rpcs;
}

ControlPayload oneControlResponse(AxtpCore core, ControlPayload control) {
  final chunks = <Bytes>[];
  OutboundProcessor(chunks.add).sendControl(control);
  for (final chunk in chunks) {
    core.byteSink.onBytes(chunk);
  }
  final outgoing = core.tryPopOutboundBytes();
  if (outgoing == null) {
    throw StateError('core did not emit a control response');
  }
  final sink = CapturingPayloadSink();
  InboundProcessor(sink).onBytes(outgoing);
  if (sink.controls.length != 1) {
    throw StateError('expected exactly one decoded control response');
  }
  return sink.controls.single;
}

bool testOpenAccept() {
  final core = AxtpCore();
  final response = oneControlResponse(
    core,
    ControlPayload(opcode: ControlOpcode.open, controlId: 1),
  );
  return response.opcode == ControlOpcode.accept &&
      response.controlId == 1 &&
      response.statusCode == ErrorCode.success &&
      core.controlSessionOpen;
}

bool testClose() {
  final core = AxtpCore();
  oneControlResponse(
      core, ControlPayload(opcode: ControlOpcode.open, controlId: 1));
  final response = oneControlResponse(
    core,
    ControlPayload(opcode: ControlOpcode.close, controlId: 2),
  );
  return response.opcode == ControlOpcode.closeAck &&
      response.controlId == 2 &&
      !core.controlSessionOpen;
}

bool testPingPong() {
  final core = AxtpCore();
  final response = oneControlResponse(
    core,
    ControlPayload(opcode: ControlOpcode.ping, controlId: 3),
  );
  return response.opcode == ControlOpcode.pong && response.controlId == 3;
}

bool testRequestResponseJson() {
  final responses = roundTripRequest(
    RpcPayload(
      encoding: RpcEncoding.json,
      op: RpcOp.request,
      requestId: 1,
      methodOrEventId: MethodId.audioGetAlgorithmConfig.value,
      bodyEncoding: RpcBodyEncoding.noneValue,
      body: utf8.encode('{}'),
    ),
    configureBroker: (broker) {
      broker.registerJsonMethod('audio.getAlgorithmConfig', (context, params) {
        if (context.methodName != 'audio.getAlgorithmConfig' ||
            params != '{}') {
          throw StateError('unexpected JSON handler context');
        }
        return '{"noiseSuppression":{"enabled":true,"level":3}}';
      });
    },
  );
  if (responses.length != 1) return false;
  final response = responses.single;
  final body = jsonDecode(utf8.decode(response.body)) as Map<String, Object?>;
  return response.op == RpcOp.requestResponse &&
      response.requestId == 1 &&
      response.statusCode == ErrorCode.success &&
      body.containsKey('noiseSuppression');
}

bool testMethodNotFoundWithId(int requestId) {
  final responses = roundTripRequest(
    RpcPayload(
      encoding: RpcEncoding.json,
      op: RpcOp.request,
      requestId: requestId,
      methodOrEventId: 0x7fff,
      bodyEncoding: RpcBodyEncoding.noneValue,
      body: utf8.encode('{}'),
    ),
  );
  return responses.length == 1 &&
      responses.single.op == RpcOp.requestResponse &&
      responses.single.requestId == requestId &&
      responses.single.statusCode == ErrorCode.rpcMethodNotFound;
}

bool testEventEmit() {
  final chunks = <Bytes>[];
  final outbound = OutboundProcessor(chunks.add)
    ..wireMode = AxtpWireMode.webSocketJsonRpc;
  outbound.sendEvent(
    RpcPayload(
      encoding: RpcEncoding.json,
      op: RpcOp.event,
      methodOrEventId: EventId.audioAlgorithmConfigChanged.value,
      bodyEncoding: RpcBodyEncoding.noneValue,
      meta: const PayloadMeta(
        sourceProtocol: SourceProtocol.jsonRpc,
        jsonSid: 's1',
      ),
      body: utf8.encode('{"reason":"user_request","applyState":"applied"}'),
    ),
  );
  if (chunks.length != 1) return false;
  final event = jsonDecode(utf8.decode(chunks.single)) as Map<String, Object?>;
  final data = event['d'] as Map<String, Object?>;
  final body = data['data'] as Map<String, Object?>;
  return event['op'] == RpcOp.event.value &&
      data['event'] == 'audio.algorithmConfigChanged' &&
      body['reason'] == 'user_request';
}

bool testCapabilityGetAll() {
  return kMethodRegistry.length >= 4 &&
      RegistryLookup.methodIdByName('audio.getAlgorithmConfig') ==
          MethodId.audioGetAlgorithmConfig.value &&
      RegistryLookup.methodIdByName('audio.getAlgorithmCapabilities') ==
          MethodId.audioGetAlgorithmCapabilities.value &&
      RegistryLookup.methodIdByName('audio.setAlgorithmConfig') ==
          MethodId.audioSetAlgorithmConfig.value &&
      RegistryLookup.methodIdByName('audio.resetAlgorithmConfig') ==
          MethodId.audioResetAlgorithmConfig.value;
}

bool testCapabilityMethodBinding() {
  final capability = kCapabilityRegistry.where((item) {
    return item.id == CapabilityId.audioAlgorithm.value &&
        item.name == 'audio.algorithm';
  }).toList();
  final method =
      RegistryLookup.methodById(MethodId.audioGetAlgorithmConfig.value);
  final event =
      RegistryLookup.eventById(EventId.audioAlgorithmConfigChanged.value);
  return capability.length == 1 &&
      method?.domain == 'audio' &&
      event?.domain == 'audio';
}

bool testStreamData() {
  final chunks = <Bytes>[];
  OutboundProcessor(chunks.add).sendStream(
    StreamPayload(
        streamId: 9, seqId: 1, cursor: 0, data: <int>[0xaa, 0xbb, 0xcc]),
  );
  final sink = CapturingPayloadSink();
  final inbound = InboundProcessor(sink);
  for (final chunk in chunks) {
    inbound.onBytes(chunk);
  }
  return sink.streams.length == 1 &&
      sink.streams.single.streamId == 9 &&
      sink.streams.single.seqId == 1 &&
      sink.streams.single.cursor == 0 &&
      bytesEqual(sink.streams.single.data, <int>[0xaa, 0xbb, 0xcc]);
}

void writeResult(String outputPath, String profilePath) {
  final summary = <String, int>{
    'total': cases.length,
    'passed': cases.where((item) => item.status == CaseStatus.passed).length,
    'failed': cases
        .where((item) =>
            item.status == CaseStatus.failed ||
            item.status == CaseStatus.pending)
        .length,
    'skipped': cases.where((item) => item.status == CaseStatus.skipped).length,
    'unsupported':
        cases.where((item) => item.status == CaseStatus.unsupported).length,
  };
  final result = <String, Object?>{
    'runtime': 'axtp-flutter-runtime',
    'runtimeVersion': AxtpGeneratedVersion.runtimeVersion,
    'specTag': AxtpGeneratedVersion.specTag,
    'profile': profilePath,
    'requiredLevels': <String>['core', 'framed-binary'],
    'optionalLevels': <String>[
      'capability',
      'websocket-jsonrpc',
      'event',
      'stream',
    ],
    'unsupportedLevels': <String>[],
    'summary': summary,
    'cases': cases.map((item) => item.toJson()).toList(),
  };
  File(outputPath)
    ..parent.createSync(recursive: true)
    ..writeAsStringSync(
        '${const JsonEncoder.withIndent('  ').convert(result)}\n');
}

bool envIsTrue(String name) => Platform.environment[name] == 'true';

String? resolveSpecPath() {
  final envPath = Platform.environment['AXTP_SPEC_PATH'];
  for (final path in <String?>[
    envPath,
    'third_party/axtp-spec',
    '.axtp-spec',
  ]) {
    if (path != null &&
        (File('$path/docs/conformance/manifest.yaml').existsSync() ||
            File('$path/conformance/manifest.yaml').existsSync())) {
      return path;
    }
  }
  return null;
}

void main() {
  test('AXTP conformance', () {
    final specPath = resolveSpecPath();
    final profilePath = Platform.environment['CONFORMANCE_PROFILE_PATH'] ??
        'conformance/runtime-profile.yaml';
    final resultPath = Platform.environment['CONFORMANCE_RESULT_PATH'] ??
        'conformance-results/result.json';

    if (specPath == null) {
      fail('AXTP conformance manifest not found');
    }
    if (!File(profilePath).existsSync()) {
      fail('runtime conformance profile not found: $profilePath');
    }

    runCase('handshake.open_accept', testOpenAccept);
    runCase('handshake.close', testClose);
    runCase('handshake.ping_pong', testPingPong);
    runCase('rpc.request_response_json', testRequestResponseJson);
    runCase('rpc.method_not_found', () => testMethodNotFoundWithId(2));
    runCase('rpc.request_id_match', () => testMethodNotFoundWithId(55));
    runCase('event.emit_event', testEventEmit);
    runCase('capability.get_all', testCapabilityGetAll);
    runCase('capability.method_binding', testCapabilityMethodBinding);
    runCase('capability.unsupported_method', () => testMethodNotFoundWithId(4));
    runCase('error.standard_error_shape', () => testMethodNotFoundWithId(99));
    runCase('stream.stream_data', testStreamData);

    writeResult(resultPath, profilePath);

    final requiredIssue = cases.any((item) =>
        item.requirement == Requirement.required &&
        item.status != CaseStatus.passed);
    final optionalIssue = cases.any((item) =>
        item.requirement == Requirement.optional &&
        item.status != CaseStatus.passed);
    if (requiredIssue && !envIsTrue('CONFORMANCE_ALLOW_INCOMPLETE')) {
      fail('required AXTP conformance cases failed');
    }
    if (optionalIssue && envIsTrue('CONFORMANCE_STRICT_OPTIONAL')) {
      fail('optional AXTP conformance cases failed or were skipped');
    }
  });
}
