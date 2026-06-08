import 'dart:typed_data';

import 'generated/axtp_registry_generated.dart';

typedef Bytes = Uint8List;

Bytes bytesFrom(Iterable<int>? bytes) => Uint8List.fromList(
      bytes == null ? const <int>[] : List<int>.from(bytes),
    );

enum SourceProtocol { axtpV1, jsonRpc }

enum TransportKind { tcp, webSocket, hid, ble, uart, mock, custom }

enum AxtpWireMode { framedBinary, webSocketJsonRpc }

const int kRpcEncodingJsonBinaryValue = 0x04;

RpcEncoding get rpcEncodingJsonBinary =>
    RpcEncoding.fromValue(kRpcEncodingJsonBinaryValue)!;

bool isJsonBinaryRpcEncoding(RpcEncoding encoding) =>
    encoding.value == kRpcEncodingJsonBinaryValue;

RpcBodyEncoding bodyEncodingForRpcEncoding(RpcEncoding encoding) =>
    isJsonBinaryRpcEncoding(encoding)
        ? RpcBodyEncoding.tlv8
        : RpcBodyEncoding.noneValue;

class TransportProfile {
  const TransportProfile({
    this.kind = TransportKind.custom,
    this.wireMode = AxtpWireMode.framedBinary,
    this.defaultRpcEncoding = RpcEncoding.json,
    this.messageOriented = false,
    this.supportsTextMessage = false,
    this.supportsBinaryMessage = true,
    this.preferredFrameSize = 4096,
  });

  final TransportKind kind;
  final AxtpWireMode wireMode;
  final RpcEncoding defaultRpcEncoding;
  final bool messageOriented;
  final bool supportsTextMessage;
  final bool supportsBinaryMessage;
  final int preferredFrameSize;
}

class PayloadMeta {
  const PayloadMeta({
    this.sourceProtocol = SourceProtocol.axtpV1,
    this.sessionId = 0,
    this.requestId = 0,
    this.jsonSid = '',
    this.jsonMethodOrEventName = '',
  });

  final SourceProtocol sourceProtocol;
  final int sessionId;
  final int requestId;
  final String jsonSid;
  final String jsonMethodOrEventName;

  PayloadMeta copyWith({
    SourceProtocol? sourceProtocol,
    int? sessionId,
    int? requestId,
    String? jsonSid,
    String? jsonMethodOrEventName,
  }) {
    return PayloadMeta(
      sourceProtocol: sourceProtocol ?? this.sourceProtocol,
      sessionId: sessionId ?? this.sessionId,
      requestId: requestId ?? this.requestId,
      jsonSid: jsonSid ?? this.jsonSid,
      jsonMethodOrEventName:
          jsonMethodOrEventName ?? this.jsonMethodOrEventName,
    );
  }
}

class ControlPayload {
  ControlPayload({
    this.opcode = ControlOpcode.open,
    this.controlId = 0,
    this.statusCode = ErrorCode.success,
    this.meta = const PayloadMeta(),
    Iterable<int>? body,
  }) : body = bytesFrom(body);

  final ControlOpcode opcode;
  final int controlId;
  final ErrorCode statusCode;
  final PayloadMeta meta;
  final Bytes body;

  ControlPayload copyWith({
    ControlOpcode? opcode,
    int? controlId,
    ErrorCode? statusCode,
    PayloadMeta? meta,
    Iterable<int>? body,
  }) {
    return ControlPayload(
      opcode: opcode ?? this.opcode,
      controlId: controlId ?? this.controlId,
      statusCode: statusCode ?? this.statusCode,
      meta: meta ?? this.meta,
      body: body ?? this.body,
    );
  }
}

class RpcPayload {
  RpcPayload({
    this.encoding = RpcEncoding.json,
    this.op = RpcOp.request,
    this.requestId = 0,
    this.methodOrEventId = 0,
    this.statusCode = ErrorCode.success,
    this.bodyEncoding = RpcBodyEncoding.noneValue,
    this.meta = const PayloadMeta(),
    Iterable<int>? body,
  }) : body = bytesFrom(body);

  final RpcEncoding encoding;
  final RpcOp op;
  final int requestId;
  final int methodOrEventId;
  final ErrorCode statusCode;
  final RpcBodyEncoding bodyEncoding;
  final PayloadMeta meta;
  final Bytes body;

  RpcPayload copyWith({
    RpcEncoding? encoding,
    RpcOp? op,
    int? requestId,
    int? methodOrEventId,
    ErrorCode? statusCode,
    RpcBodyEncoding? bodyEncoding,
    PayloadMeta? meta,
    Iterable<int>? body,
  }) {
    return RpcPayload(
      encoding: encoding ?? this.encoding,
      op: op ?? this.op,
      requestId: requestId ?? this.requestId,
      methodOrEventId: methodOrEventId ?? this.methodOrEventId,
      statusCode: statusCode ?? this.statusCode,
      bodyEncoding: bodyEncoding ?? this.bodyEncoding,
      meta: meta ?? this.meta,
      body: body ?? this.body,
    );
  }
}

class StreamPayload {
  StreamPayload({
    this.streamId = 0,
    this.seqId = 0,
    this.cursor = 0,
    this.meta = const PayloadMeta(),
    Iterable<int>? data,
  }) : data = bytesFrom(data);

  final int streamId;
  final int seqId;
  final int cursor;
  final PayloadMeta meta;
  final Bytes data;

  StreamPayload copyWith({
    int? streamId,
    int? seqId,
    int? cursor,
    PayloadMeta? meta,
    Iterable<int>? data,
  }) {
    return StreamPayload(
      streamId: streamId ?? this.streamId,
      seqId: seqId ?? this.seqId,
      cursor: cursor ?? this.cursor,
      meta: meta ?? this.meta,
      data: data ?? this.data,
    );
  }
}

enum CoreEventType {
  rpcRequest,
  rpcEvent,
  streamOpen,
  streamData,
  streamClose,
  controlNotice,
  protocolError,
}

class CoreEvent {
  CoreEvent({
    required this.type,
    RpcPayload? rpc,
    StreamPayload? stream,
    ControlPayload? control,
    this.error = ErrorCode.success,
  })  : rpc = rpc ?? RpcPayload(),
        stream = stream ?? StreamPayload(),
        control = control ?? ControlPayload();

  factory CoreEvent.rpcRequest(RpcPayload payload) =>
      CoreEvent(type: CoreEventType.rpcRequest, rpc: payload);

  factory CoreEvent.rpcEvent(RpcPayload payload) =>
      CoreEvent(type: CoreEventType.rpcEvent, rpc: payload);

  factory CoreEvent.streamData(StreamPayload payload) =>
      CoreEvent(type: CoreEventType.streamData, stream: payload);

  factory CoreEvent.controlNotice(ControlPayload payload) =>
      CoreEvent(type: CoreEventType.controlNotice, control: payload);

  factory CoreEvent.protocolError(ErrorCode error) =>
      CoreEvent(type: CoreEventType.protocolError, error: error);

  final CoreEventType type;
  final RpcPayload rpc;
  final StreamPayload stream;
  final ControlPayload control;
  final ErrorCode error;
}
