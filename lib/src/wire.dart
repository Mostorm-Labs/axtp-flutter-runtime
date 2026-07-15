import 'dart:collection';
import 'dart:convert';
import 'dart:math';

import 'generated/axtp_registry_generated.dart';
import 'model.dart';
import 'transport.dart';

const int kAxtpStandardMagic0 = 0x41;
const int kAxtpStandardMagic1 = 0x58;
const int kAxtpVersion1 = 0x01;
const int kStandardFrameHeaderSize = 12;
const int kStandardFrameCrcSize = 2;
const int kControlPayloadHeaderSize = 5;
const int kBinaryRpcHeaderSize = 11;
const int kStreamPayloadHeaderSize = 16;

int crc16CcittFalse(Iterable<int> data) {
  var crc = 0xffff;
  for (final byte in data) {
    crc ^= (byte & 0xff) << 8;
    for (var bit = 0; bit < 8; bit++) {
      if ((crc & 0x8000) != 0) {
        crc = ((crc << 1) ^ 0x1021) & 0xffff;
      } else {
        crc = (crc << 1) & 0xffff;
      }
    }
  }
  return crc;
}

class ByteWriter {
  final List<int> _bytes = <int>[];

  List<int> get bytes => List<int>.unmodifiable(_bytes);

  void writeU8(int value) {
    _bytes.add(value & 0xff);
  }

  void writeU16(int value) {
    _bytes.add((value >> 8) & 0xff);
    _bytes.add(value & 0xff);
  }

  void writeU32(int value) {
    for (var shift = 24; shift >= 0; shift -= 8) {
      _bytes.add((value >> shift) & 0xff);
    }
  }

  void writeU64(int value) {
    for (var shift = 56; shift >= 0; shift -= 8) {
      _bytes.add((value >> shift) & 0xff);
    }
  }

  void writeBytes(Iterable<int> value) {
    _bytes.addAll(value.map((byte) => byte & 0xff));
  }

  Bytes takeBytes() {
    final out = bytesFrom(_bytes);
    _bytes.clear();
    return out;
  }
}

class ByteReader {
  ByteReader(Iterable<int> bytes) : _bytes = List<int>.from(bytes);

  final List<int> _bytes;
  int _offset = 0;

  int get remaining => _bytes.length - _offset;

  int? readU8() {
    if (remaining < 1) return null;
    return _bytes[_offset++] & 0xff;
  }

  int? readU16() {
    if (remaining < 2) return null;
    final value =
        ((_bytes[_offset] & 0xff) << 8) | (_bytes[_offset + 1] & 0xff);
    _offset += 2;
    return value;
  }

  int? readU32() {
    if (remaining < 4) return null;
    var value = 0;
    for (var shift = 24; shift >= 0; shift -= 8) {
      value |= (_bytes[_offset++] & 0xff) << shift;
    }
    return value;
  }

  int? readU64() {
    if (remaining < 8) return null;
    var value = 0;
    for (var shift = 56; shift >= 0; shift -= 8) {
      value |= (_bytes[_offset++] & 0xff) << shift;
    }
    return value;
  }

  Bytes? readBytes(int count) {
    if (remaining < count) return null;
    final out = bytesFrom(_bytes.sublist(_offset, _offset + count));
    _offset += count;
    return out;
  }
}

class FrameHeader {
  const FrameHeader({
    this.version = kAxtpVersion1,
    this.payloadType = PayloadType.rpc,
    this.payloadLength = 0,
    this.sourceId = 0,
    this.destinationId = 0,
    this.messageId = 0,
    this.frameIndex = 0,
    this.frameCount = 1,
  });

  final int version;
  final PayloadType payloadType;
  final int payloadLength;
  final int sourceId;
  final int destinationId;
  final int messageId;
  final int frameIndex;
  final int frameCount;

  FrameHeader copyWith({
    int? version,
    PayloadType? payloadType,
    int? payloadLength,
    int? sourceId,
    int? destinationId,
    int? messageId,
    int? frameIndex,
    int? frameCount,
  }) {
    return FrameHeader(
      version: version ?? this.version,
      payloadType: payloadType ?? this.payloadType,
      payloadLength: payloadLength ?? this.payloadLength,
      sourceId: sourceId ?? this.sourceId,
      destinationId: destinationId ?? this.destinationId,
      messageId: messageId ?? this.messageId,
      frameIndex: frameIndex ?? this.frameIndex,
      frameCount: frameCount ?? this.frameCount,
    );
  }
}

class Frame {
  Frame({
    FrameHeader? header,
    Iterable<int>? payload,
    this.crc16 = 0,
  })  : header = header ?? const FrameHeader(),
        payload = bytesFrom(payload);

  final FrameHeader header;
  final Bytes payload;
  final int crc16;
}

class Message {
  Message({
    this.messageId = 0,
    this.payloadType = PayloadType.rpc,
    Iterable<int>? body,
  }) : body = bytesFrom(body);

  final int messageId;
  final PayloadType payloadType;
  final Bytes body;
}

class FrameDecoder implements ByteSink {
  FrameDecoder(this._onFrame);

  final void Function(Frame frame) _onFrame;
  final List<int> _buffer = <int>[];

  @override
  void onBytes(Bytes bytes) {
    _buffer.addAll(bytes);
    _parseLoop();
  }

  static bool _isPayloadType(int value) => PayloadType.fromValue(value) != null;

  void _consume(int count) {
    if (count > 0) _buffer.removeRange(0, min(count, _buffer.length));
  }

  void _resyncToMagic() {
    for (var i = 0; i + 1 < _buffer.length; i++) {
      if (_buffer[i] == kAxtpStandardMagic0 &&
          _buffer[i + 1] == kAxtpStandardMagic1) {
        _consume(i);
        return;
      }
    }
    if (_buffer.isEmpty) return;
    final keep = _buffer.last == kAxtpStandardMagic0 ? 1 : 0;
    _consume(_buffer.length - keep);
  }

  void _parseLoop() {
    while (true) {
      _resyncToMagic();
      if (_buffer.length < kStandardFrameHeaderSize + kStandardFrameCrcSize) {
        return;
      }

      final headerBytes = _buffer.sublist(0, kStandardFrameHeaderSize);
      final reader = ByteReader(headerBytes);
      reader.readU8();
      reader.readU8();
      final version = reader.readU8();
      final payloadTypeValue = reader.readU8();
      final payloadLength = reader.readU16();
      final sourceId = reader.readU8();
      final destinationId = reader.readU8();
      final messageId = reader.readU16();
      final frameIndex = reader.readU8();
      final frameCount = reader.readU8();

      if (version == null ||
          payloadTypeValue == null ||
          payloadLength == null ||
          sourceId == null ||
          destinationId == null ||
          messageId == null ||
          frameIndex == null ||
          frameCount == null) {
        return;
      }

      if (version != kAxtpVersion1 ||
          !_isPayloadType(payloadTypeValue) ||
          frameCount == 0 ||
          frameIndex >= frameCount) {
        _consume(1);
        continue;
      }

      final totalSize =
          kStandardFrameHeaderSize + payloadLength + kStandardFrameCrcSize;
      if (_buffer.length < totalSize) return;

      final frameBytes = _buffer.sublist(0, totalSize);
      final expectedCrc =
          ByteReader(frameBytes.sublist(totalSize - kStandardFrameCrcSize))
              .readU16();
      final actualCrc = crc16CcittFalse(
          frameBytes.sublist(0, totalSize - kStandardFrameCrcSize));
      if (expectedCrc == null || expectedCrc != actualCrc) {
        _consume(1);
        continue;
      }

      final payload = frameBytes.sublist(
        kStandardFrameHeaderSize,
        kStandardFrameHeaderSize + payloadLength,
      );
      _consume(totalSize);
      _onFrame(
        Frame(
          header: FrameHeader(
            version: version,
            payloadType: PayloadType.fromValue(payloadTypeValue)!,
            payloadLength: payloadLength,
            sourceId: sourceId,
            destinationId: destinationId,
            messageId: messageId,
            frameIndex: frameIndex,
            frameCount: frameCount,
          ),
          payload: payload,
          crc16: expectedCrc,
        ),
      );
    }
  }
}

class FrameEncoder {
  Bytes encode(Frame frame) {
    final writer = ByteWriter()
      ..writeU8(kAxtpStandardMagic0)
      ..writeU8(kAxtpStandardMagic1)
      ..writeU8(frame.header.version)
      ..writeU8(frame.header.payloadType.value)
      ..writeU16(frame.payload.length)
      ..writeU8(frame.header.sourceId)
      ..writeU8(frame.header.destinationId)
      ..writeU16(frame.header.messageId)
      ..writeU8(frame.header.frameIndex)
      ..writeU8(frame.header.frameCount)
      ..writeBytes(frame.payload);
    writer.writeU16(crc16CcittFalse(writer.bytes));
    return writer.takeBytes();
  }
}

class MessageReassembler {
  MessageReassembler(this._onMessage, {this.maxMessageSize = 1024 * 1024});

  final void Function(Message message) _onMessage;
  final int maxMessageSize;
  final Map<int, _Assembly> _assemblies = <int, _Assembly>{};

  void onFrame(Frame frame) {
    if (frame.header.frameCount == 1) {
      if (frame.header.frameIndex != 0) return;
      _onMessage(
        Message(
          messageId: frame.header.messageId,
          payloadType: frame.header.payloadType,
          body: frame.payload,
        ),
      );
      return;
    }

    final assembly = _assemblies.putIfAbsent(
      frame.header.messageId,
      () => _Assembly(frame.header.payloadType, frame.header.frameCount),
    );
    if (assembly.payloadType != frame.header.payloadType ||
        assembly.frameCount != frame.header.frameCount ||
        frame.header.frameIndex >= assembly.fragments.length) {
      _assemblies.remove(frame.header.messageId);
      return;
    }

    if (assembly.fragments[frame.header.frameIndex] != null) return;
    assembly.totalSize += frame.payload.length;
    if (assembly.totalSize > maxMessageSize) {
      _assemblies.remove(frame.header.messageId);
      return;
    }
    assembly.fragments[frame.header.frameIndex] = frame.payload;
    if (assembly.fragments.any((fragment) => fragment == null)) return;

    final writer = ByteWriter();
    for (final fragment in assembly.fragments) {
      writer.writeBytes(fragment!);
    }
    _assemblies.remove(frame.header.messageId);
    _onMessage(
      Message(
        messageId: frame.header.messageId,
        payloadType: assembly.payloadType,
        body: writer.takeBytes(),
      ),
    );
  }
}

class _Assembly {
  _Assembly(this.payloadType, this.frameCount)
      : fragments = List<Bytes?>.filled(frameCount, null);

  final PayloadType payloadType;
  final int frameCount;
  final List<Bytes?> fragments;
  int totalSize = 0;
}

class MessageFragmenter {
  MessageFragmenter({this.maxFrameSize = 4096});

  int maxFrameSize;
  int _nextMessageId = 1;

  List<Frame> fragment(Message message) {
    final maxPayloadSize = _payloadCapacity;
    if (maxPayloadSize == 0 || message.body.isEmpty) {
      return <Frame>[
        _makeFrame(message, _takeMessageId(), 0, 1, const <int>[])
      ];
    }

    final frameCount =
        ((message.body.length + maxPayloadSize - 1) ~/ maxPayloadSize);
    if (frameCount > 255) {
      throw RangeError.value(frameCount, 'frameCount', 'must fit in uint8');
    }
    final messageId = _takeMessageId();
    final frames = <Frame>[];
    for (var index = 0; index < frameCount; index++) {
      final offset = index * maxPayloadSize;
      final chunkSize = min(maxPayloadSize, message.body.length - offset);
      frames.add(
        _makeFrame(
          message,
          messageId,
          index,
          frameCount,
          message.body.sublist(offset, offset + chunkSize),
        ),
      );
    }
    return frames;
  }

  int get _payloadCapacity {
    if (maxFrameSize <= kStandardFrameHeaderSize + kStandardFrameCrcSize) {
      return 0;
    }
    return maxFrameSize - kStandardFrameHeaderSize - kStandardFrameCrcSize;
  }

  int _takeMessageId() {
    final id = _nextMessageId;
    _nextMessageId = (_nextMessageId + 1) & 0xffff;
    if (_nextMessageId == 0) _nextMessageId = 1;
    return id;
  }

  Frame _makeFrame(
    Message message,
    int messageId,
    int frameIndex,
    int frameCount,
    Iterable<int> payload,
  ) {
    final body = bytesFrom(payload);
    return Frame(
      header: FrameHeader(
        payloadType: message.payloadType,
        payloadLength: body.length,
        messageId: messageId,
        frameIndex: frameIndex,
        frameCount: frameCount,
      ),
      payload: body,
    );
  }
}

abstract class PayloadSink {
  void onControl(ControlPayload payload);

  void onRpc(RpcPayload payload);

  void onStream(StreamPayload payload);
}

class PayloadEncoder {
  Message encodeControl(ControlPayload payload) {
    final writer = ByteWriter()
      ..writeU8(payload.opcode.value)
      ..writeU16(payload.controlId)
      ..writeU16(payload.statusCode.value)
      ..writeBytes(payload.body);
    return Message(payloadType: PayloadType.control, body: writer.takeBytes());
  }

  Message encodeRpc(RpcPayload payload) {
    final writer = ByteWriter()
      ..writeU8(payload.encoding.value)
      ..writeU8(payload.op.value)
      ..writeU32(payload.requestId)
      ..writeU16(payload.methodOrEventId)
      ..writeU16(payload.statusCode.value)
      ..writeU8(payload.bodyEncoding.value)
      ..writeBytes(payload.body);
    return Message(payloadType: PayloadType.rpc, body: writer.takeBytes());
  }

  Message encodeStream(StreamPayload payload) {
    final writer = ByteWriter()
      ..writeU32(payload.streamId)
      ..writeU32(payload.seqId)
      ..writeU64(payload.cursor)
      ..writeBytes(payload.data);
    return Message(payloadType: PayloadType.stream, body: writer.takeBytes());
  }
}

class PayloadDecoder {
  PayloadDecoder(this._sink);

  final PayloadSink _sink;

  void onMessage(Message message) {
    switch (message.payloadType) {
      case PayloadType.control:
        _decodeControl(message);
      case PayloadType.rpc:
        _decodeRpc(message);
      case PayloadType.stream:
        _decodeStream(message);
    }
  }

  void _decodeControl(Message message) {
    if (message.body.length < kControlPayloadHeaderSize) return;
    final reader = ByteReader(message.body);
    final opcode = reader.readU8();
    final controlId = reader.readU16();
    final statusCode = reader.readU16();
    final body = reader.readBytes(reader.remaining);
    final parsedOpcode =
        opcode == null ? null : ControlOpcode.fromValue(opcode);
    final parsedStatus =
        statusCode == null ? null : ErrorCode.fromValue(statusCode);
    if (parsedOpcode == null ||
        controlId == null ||
        parsedStatus == null ||
        body == null) {
      return;
    }
    _sink.onControl(
      ControlPayload(
        opcode: parsedOpcode,
        controlId: controlId,
        statusCode: parsedStatus,
        body: body,
      ),
    );
  }

  void _decodeRpc(Message message) {
    if (message.body.length < kBinaryRpcHeaderSize) return;
    final reader = ByteReader(message.body);
    final encoding = reader.readU8();
    final op = reader.readU8();
    final requestId = reader.readU32();
    final methodOrEventId = reader.readU16();
    final statusCode = reader.readU16();
    final bodyEncoding = reader.readU8();
    final body = reader.readBytes(reader.remaining);
    final parsedEncoding =
        encoding == null ? null : RpcEncoding.fromValue(encoding);
    final parsedOp = op == null ? null : RpcOp.fromValue(op);
    final parsedStatus =
        statusCode == null ? null : ErrorCode.fromValue(statusCode);
    final parsedBodyEncoding =
        bodyEncoding == null ? null : RpcBodyEncoding.fromValue(bodyEncoding);
    if (parsedEncoding == null ||
        parsedOp == null ||
        requestId == null ||
        methodOrEventId == null ||
        parsedStatus == null ||
        parsedBodyEncoding == null ||
        body == null) {
      return;
    }
    _sink.onRpc(
      RpcPayload(
        encoding: parsedEncoding,
        op: parsedOp,
        requestId: requestId,
        methodOrEventId: methodOrEventId,
        statusCode: parsedStatus,
        bodyEncoding: parsedBodyEncoding,
        meta: PayloadMeta(requestId: requestId),
        body: body,
      ),
    );
  }

  void _decodeStream(Message message) {
    if (message.body.length < kStreamPayloadHeaderSize) return;
    final reader = ByteReader(message.body);
    final streamId = reader.readU32();
    final seqId = reader.readU32();
    final cursor = reader.readU64();
    final data = reader.readBytes(reader.remaining);
    if (streamId == null || seqId == null || cursor == null || data == null) {
      return;
    }
    _sink.onStream(
      StreamPayload(
          streamId: streamId, seqId: seqId, cursor: cursor, data: data),
    );
  }
}

class JsonRpcDecoder implements ByteSink {
  JsonRpcDecoder(this._sink);

  final PayloadSink _sink;

  @override
  void onBytes(Bytes bytes) {
    try {
      final decoded = jsonDecode(utf8.decode(bytes));
      if (decoded is! Map<String, Object?>) return;
      final op = _parseOp(decoded);
      final data = decoded['d'];
      if (data is! Map<String, Object?>) return;

      switch (op) {
        case RpcOp.request:
          _decodeRequest(decoded, data);
        case RpcOp.event:
          _decodeEvent(decoded, data);
        case RpcOp.identify:
        case RpcOp.reidentify:
          _decodeSessionRpc(decoded, data, op);
        case RpcOp.hello:
          // axtpVersion is advisory: absent, malformed, or newer values do
          // not gate the session. Preserve the Hello for the session layer.
          _decodeSessionRpc(decoded, data, op);
        case RpcOp.requestBatch:
          _decodeBatch(decoded, data);
        default:
          return;
      }
    } catch (_) {
      return;
    }
  }

  RpcOp _parseOp(Map<String, Object?> object) {
    final raw = object['op'];
    if (raw is! int || raw < 0 || raw > 0xff) {
      throw const FormatException('invalid op');
    }
    final op = RpcOp.fromValue(raw);
    if (op == null) throw const FormatException('unknown op');
    return op;
  }

  String _parseSid(Map<String, Object?> object) {
    final sid = object['sid'];
    return sid is String ? sid : '';
  }

  int _parseRequestId(Map<String, Object?> data) {
    final raw = data['id'];
    if (raw is! int || raw <= 0 || raw > 0xffffffff) {
      throw const FormatException('invalid id');
    }
    return raw;
  }

  Bytes _jsonToBytes(Object? value) =>
      bytesFrom(utf8.encode(jsonEncode(value)));

  void _decodeRequest(
    Map<String, Object?> object,
    Map<String, Object?> data,
  ) {
    final method = data['method'];
    if (method is! String) throw const FormatException('missing method');
    final requestId = _parseRequestId(data);
    final methodId = RegistryLookup.methodIdByName(method);
    if (methodId == null) {
      _sink.onRpc(
        RpcPayload(
          encoding: RpcEncoding.json,
          op: RpcOp.requestResponse,
          requestId: requestId,
          statusCode: ErrorCode.rpcMethodNotFound,
          bodyEncoding: RpcBodyEncoding.noneValue,
          meta: PayloadMeta(
            sourceProtocol: SourceProtocol.jsonRpc,
            jsonSid: _parseSid(object),
            jsonMethodOrEventName: method,
          ),
        ),
      );
      return;
    }

    _sink.onRpc(
      RpcPayload(
        encoding: RpcEncoding.json,
        op: RpcOp.request,
        requestId: requestId,
        methodOrEventId: methodId,
        bodyEncoding: RpcBodyEncoding.noneValue,
        meta: PayloadMeta(
          sourceProtocol: SourceProtocol.jsonRpc,
          requestId: requestId,
          jsonSid: _parseSid(object),
          jsonMethodOrEventName: method,
        ),
        body: data.containsKey('params') ? _jsonToBytes(data['params']) : null,
      ),
    );
  }

  void _decodeEvent(
    Map<String, Object?> object,
    Map<String, Object?> data,
  ) {
    final event = data['event'];
    if (event is! String) throw const FormatException('missing event');
    final eventId = RegistryLookup.eventIdByName(event);
    if (eventId == null) return;
    _sink.onRpc(
      RpcPayload(
        encoding: RpcEncoding.json,
        op: RpcOp.event,
        methodOrEventId: eventId,
        bodyEncoding: RpcBodyEncoding.noneValue,
        meta: PayloadMeta(
          sourceProtocol: SourceProtocol.jsonRpc,
          jsonSid: _parseSid(object),
          jsonMethodOrEventName: event,
        ),
        body: data.containsKey('data') ? _jsonToBytes(data['data']) : null,
      ),
    );
  }

  void _decodeSessionRpc(
    Map<String, Object?> object,
    Map<String, Object?> data,
    RpcOp op,
  ) {
    _sink.onRpc(
      RpcPayload(
        encoding: RpcEncoding.json,
        op: op,
        bodyEncoding: RpcBodyEncoding.noneValue,
        meta: PayloadMeta(
          sourceProtocol: SourceProtocol.jsonRpc,
          jsonSid: _parseSid(object),
        ),
        body: _jsonToBytes(data),
      ),
    );
  }

  void _decodeBatch(Map<String, Object?> object, Map<String, Object?> data) {
    final requestId = _parseRequestId(data);
    _sink.onRpc(
      RpcPayload(
        encoding: RpcEncoding.json,
        op: RpcOp.requestBatchResponse,
        requestId: requestId,
        statusCode: ErrorCode.rpcBatchUnsupported,
        bodyEncoding: RpcBodyEncoding.noneValue,
        meta: PayloadMeta(
          sourceProtocol: SourceProtocol.jsonRpc,
          requestId: requestId,
          jsonSid: _parseSid(object),
        ),
        body: _jsonToBytes(data),
      ),
    );
  }
}

class JsonRpcEncoder {
  Bytes encode(RpcPayload payload) {
    final text = switch (payload.op) {
      RpcOp.hello => _serializeHello(),
      RpcOp.identified => _serializeIdentified(payload),
      RpcOp.request => _serializeRequest(payload),
      RpcOp.event => _serializeEvent(payload),
      RpcOp.requestBatchResponse => _serializeBatchResponse(payload),
      _ => _serializeResponse(payload),
    };
    return bytesFrom(utf8.encode(text));
  }

  Object? _bytesToJson(Bytes bytes) {
    if (bytes.isEmpty) return null;
    try {
      return jsonDecode(utf8.decode(bytes));
    } catch (_) {
      return null;
    }
  }

  String _errorName(ErrorCode code) =>
      RegistryLookup.errorByCode(code)?.name ?? 'UNKNOWN_ERROR';

  Map<String, Object?> _statusObject(ErrorCode code) => <String, Object?>{
        'ok': code == ErrorCode.success,
        'code': code.value,
        if (code != ErrorCode.success) 'msg': _errorName(code),
      };

  String _serializeHello() {
    return jsonEncode(<String, Object?>{
      'sid': '',
      'op': RpcOp.hello.value,
      'd': <String, Object?>{
        'axtpVersion': '1.0.0',
        'rpcVersion': 1,
      },
    });
  }

  String _serializeIdentified(RpcPayload payload) {
    return jsonEncode(<String, Object?>{
      'sid': payload.meta.jsonSid,
      'op': RpcOp.identified.value,
      'd': <String, Object?>{'negotiatedRpcVersion': 1},
    });
  }

  String _serializeRequest(RpcPayload payload) {
    final methodName = payload.meta.jsonMethodOrEventName.isNotEmpty
        ? payload.meta.jsonMethodOrEventName
        : RegistryLookup.methodById(payload.methodOrEventId)?.name ?? '';
    return jsonEncode(<String, Object?>{
      'sid': payload.meta.jsonSid,
      'op': RpcOp.request.value,
      'd': <String, Object?>{
        'id': payload.requestId,
        'method': methodName,
        if (_bytesToJson(payload.body) case final params?) 'params': params,
      },
    });
  }

  String _serializeResponse(RpcPayload payload) {
    var statusCode = payload.statusCode;
    final result = _bytesToJson(payload.body);
    if (statusCode == ErrorCode.success &&
        payload.body.isNotEmpty &&
        result == null) {
      statusCode = ErrorCode.rpcBodyDecodeFailed;
    }
    return jsonEncode(<String, Object?>{
      'sid': payload.meta.jsonSid,
      'op': RpcOp.requestResponse.value,
      'd': <String, Object?>{
        'id': payload.requestId,
        'status': _statusObject(statusCode),
        if (statusCode == ErrorCode.success && result != null) 'result': result,
      },
    });
  }

  String _serializeBatchResponse(RpcPayload payload) {
    return jsonEncode(<String, Object?>{
      'sid': payload.meta.jsonSid,
      'op': RpcOp.requestBatchResponse.value,
      'd': <String, Object?>{
        'id': payload.requestId,
        'status': _statusObject(payload.statusCode),
      },
    });
  }

  String _serializeEvent(RpcPayload payload) {
    final eventName = payload.meta.jsonMethodOrEventName.isNotEmpty
        ? payload.meta.jsonMethodOrEventName
        : RegistryLookup.eventById(payload.methodOrEventId)?.name ?? '';
    return jsonEncode(<String, Object?>{
      'sid': payload.meta.jsonSid,
      'op': RpcOp.event.value,
      'd': <String, Object?>{
        'event': eventName,
        if (_bytesToJson(payload.body) case final data?) 'data': data,
      },
    });
  }
}

class InboundProcessor implements ByteSink {
  InboundProcessor(PayloadSink sink)
      : _payloadDecoder = PayloadDecoder(sink),
        _jsonRpcDecoder = JsonRpcDecoder(sink) {
    _messageReassembler = MessageReassembler(_payloadDecoder.onMessage);
    _frameDecoder = FrameDecoder(_messageReassembler.onFrame);
  }

  final PayloadDecoder _payloadDecoder;
  final JsonRpcDecoder _jsonRpcDecoder;
  late final MessageReassembler _messageReassembler;
  late final FrameDecoder _frameDecoder;

  AxtpWireMode wireMode = AxtpWireMode.framedBinary;

  @override
  void onBytes(Bytes bytes) {
    switch (wireMode) {
      case AxtpWireMode.framedBinary:
        _frameDecoder.onBytes(bytes);
      case AxtpWireMode.webSocketJsonRpc:
        _jsonRpcDecoder.onBytes(bytes);
    }
  }
}

class OutboundProcessor {
  OutboundProcessor(this._writeBytes, {int maxFrameSize = 4096})
      : _messageFragmenter = MessageFragmenter(maxFrameSize: maxFrameSize);

  final void Function(Bytes bytes) _writeBytes;
  final PayloadEncoder _payloadEncoder = PayloadEncoder();
  final FrameEncoder _frameEncoder = FrameEncoder();
  final JsonRpcEncoder _jsonRpcEncoder = JsonRpcEncoder();
  final MessageFragmenter _messageFragmenter;

  AxtpWireMode wireMode = AxtpWireMode.framedBinary;

  set maxFrameSize(int value) {
    _messageFragmenter.maxFrameSize = value;
  }

  void sendControl(ControlPayload payload) {
    if (wireMode == AxtpWireMode.webSocketJsonRpc) return;
    _sendMessage(_payloadEncoder.encodeControl(payload));
  }

  void sendRpcRequest(RpcPayload payload) => _sendRpc(payload);

  void sendRpcResponse(RpcPayload payload) => _sendRpc(payload);

  void sendRpcError(RpcPayload payload) => _sendRpc(payload);

  void sendEvent(RpcPayload payload) => _sendRpc(payload);

  void sendStream(StreamPayload payload) {
    if (wireMode == AxtpWireMode.webSocketJsonRpc) return;
    _sendMessage(_payloadEncoder.encodeStream(payload));
  }

  void _sendRpc(RpcPayload payload) {
    if (wireMode == AxtpWireMode.webSocketJsonRpc) {
      _writeBytes(_jsonRpcEncoder.encode(payload));
      return;
    }
    _sendMessage(_payloadEncoder.encodeRpc(payload));
  }

  void _sendMessage(Message message) {
    for (final frame in _messageFragmenter.fragment(message)) {
      _writeBytes(_frameEncoder.encode(frame));
    }
  }
}

class CapturingPayloadSink implements PayloadSink {
  final List<ControlPayload> controls = <ControlPayload>[];
  final List<RpcPayload> rpcs = <RpcPayload>[];
  final List<StreamPayload> streams = <StreamPayload>[];

  @override
  void onControl(ControlPayload payload) {
    controls.add(payload);
  }

  @override
  void onRpc(RpcPayload payload) {
    rpcs.add(payload);
  }

  @override
  void onStream(StreamPayload payload) {
    streams.add(payload);
  }
}

class CapturingByteSink implements ByteSink {
  final Queue<Bytes> chunks = Queue<Bytes>();

  @override
  void onBytes(Bytes bytes) {
    chunks.add(bytesFrom(bytes));
  }
}
