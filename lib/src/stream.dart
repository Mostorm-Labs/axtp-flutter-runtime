import 'generated/axtp_registry_generated.dart';
import 'model.dart';

class StreamInfo {
  const StreamInfo({
    this.streamId = 0,
    this.kind = '',
    this.source = '',
    this.streamProfile = '',
    this.cursorUnit = '',
    this.payloadFormat = '',
    this.metadata = const <String, Object?>{},
  });

  final int streamId;
  final String kind;
  final String source;
  final String streamProfile;
  final String cursorUnit;
  final String payloadFormat;
  final Map<String, Object?> metadata;

  StreamInfo copyWith({
    int? streamId,
    String? kind,
    String? source,
    String? streamProfile,
    String? cursorUnit,
    String? payloadFormat,
    Map<String, Object?>? metadata,
  }) {
    return StreamInfo(
      streamId: streamId ?? this.streamId,
      kind: kind ?? this.kind,
      source: source ?? this.source,
      streamProfile: streamProfile ?? this.streamProfile,
      cursorUnit: cursorUnit ?? this.cursorUnit,
      payloadFormat: payloadFormat ?? this.payloadFormat,
      metadata: metadata ?? this.metadata,
    );
  }

  @override
  bool operator ==(Object other) {
    if (other is! StreamInfo) return false;
    return streamId == other.streamId &&
        kind == other.kind &&
        source == other.source &&
        streamProfile == other.streamProfile &&
        cursorUnit == other.cursorUnit &&
        payloadFormat == other.payloadFormat &&
        _mapEquals(metadata, other.metadata);
  }

  @override
  int get hashCode => Object.hash(
        streamId,
        kind,
        source,
        streamProfile,
        cursorUnit,
        payloadFormat,
        Object.hashAll(metadata.entries.map((entry) => Object.hash(entry.key, entry.value))),
      );
}

class ActiveStream {
  const ActiveStream({
    this.streamId = 0,
    this.kind = '',
    this.source = '',
    this.streamProfile = '',
  });

  final int streamId;
  final String kind;
  final String source;
  final String streamProfile;
}

class StreamStats {
  const StreamStats({
    this.chunks = 0,
    this.bytes = 0,
    this.unknownChunks = 0,
    this.seqGaps = 0,
    this.duplicateSeq = 0,
  });

  final int chunks;
  final int bytes;
  final int unknownChunks;
  final int seqGaps;
  final int duplicateSeq;

  StreamStats copyWith({
    int? chunks,
    int? bytes,
    int? unknownChunks,
    int? seqGaps,
    int? duplicateSeq,
  }) {
    return StreamStats(
      chunks: chunks ?? this.chunks,
      bytes: bytes ?? this.bytes,
      unknownChunks: unknownChunks ?? this.unknownChunks,
      seqGaps: seqGaps ?? this.seqGaps,
      duplicateSeq: duplicateSeq ?? this.duplicateSeq,
    );
  }
}

abstract interface class StreamSink {
  void onStreamOpened(StreamInfo info);
  void onStreamChunk(StreamInfo info, StreamPayload stream);
  void onStreamClosed(StreamInfo info);
}

class StreamRegistry {
  StreamRegistry({StreamSink? streamSink}) : _streamSink = streamSink;

  final StreamSink? _streamSink;
  final Map<int, _StreamContext> _streams = <int, _StreamContext>{};
  StreamStats _stats = const StreamStats();

  static bool shouldLogChunkCount(int count) => count <= 50 || count % 100 == 0;

  bool hasOpenStream(String kind, String source) {
    return _streams.values.any(
      (context) => context.info.kind == kind && context.info.source == source,
    );
  }

  bool hasStream(int streamId) => _streams.containsKey(streamId);

  StreamInfo? findStream(int streamId) => _streams[streamId]?.info;

  ErrorCode registerStream(
    StreamInfo info, {
    bool rejectDuplicateKindSource = true,
  }) {
    if (info.streamId == 0) return ErrorCode.streamIdInvalid;
    if (info.kind.isEmpty) return ErrorCode.streamPayloadInvalid;
    if (_streams.containsKey(info.streamId)) return ErrorCode.streamAlreadyOpen;
    if (rejectDuplicateKindSource && hasOpenStream(info.kind, info.source)) {
      return ErrorCode.streamAlreadyOpen;
    }

    final stored = _cloneInfo(info);
    _streams[stored.streamId] = _StreamContext(stored);
    _streamSink?.onStreamOpened(_cloneInfo(stored));
    return ErrorCode.success;
  }

  StreamInfo? close(int streamId) {
    final context = _streams.remove(streamId);
    if (context == null) return null;
    final info = _cloneInfo(context.info);
    _streamSink?.onStreamClosed(info);
    return info;
  }

  void handleStream(StreamPayload stream) {
    final context = _streams[stream.streamId];
    if (context == null) {
      _stats = _stats.copyWith(unknownChunks: _stats.unknownChunks + 1);
      return;
    }

    if (context.hasSeq) {
      if (stream.seqId == context.expectedSeq - 1) {
        _stats = _stats.copyWith(duplicateSeq: _stats.duplicateSeq + 1);
      } else if (stream.seqId != context.expectedSeq) {
        _stats = _stats.copyWith(seqGaps: _stats.seqGaps + 1);
      }
    }
    context.hasSeq = true;
    context.expectedSeq = (stream.seqId + 1) & 0xffffffff;
    context.chunks += 1;
    context.bytes += stream.data.length;
    _stats = _stats.copyWith(
      chunks: _stats.chunks + 1,
      bytes: _stats.bytes + stream.data.length,
    );
    _streamSink?.onStreamChunk(_cloneInfo(context.info), stream);
  }

  StreamStats get stats => _stats;

  int get activeStreamCount => _streams.length;

  List<ActiveStream> activeStreamsSnapshot() {
    return _streams.values
        .map(
          (context) => ActiveStream(
            streamId: context.info.streamId,
            kind: context.info.kind,
            source: context.info.source,
            streamProfile: context.info.streamProfile,
          ),
        )
        .toList();
  }
}

class _StreamContext {
  _StreamContext(this.info);

  final StreamInfo info;
  int expectedSeq = 0;
  bool hasSeq = false;
  int chunks = 0;
  int bytes = 0;
}

String toHexU32(int value) {
  return '0x${(value & 0xffffffff).toRadixString(16).toUpperCase().padLeft(8, '0')}';
}

StreamInfo _cloneInfo(StreamInfo info) {
  return info.copyWith(metadata: Map<String, Object?>.from(info.metadata));
}

bool _mapEquals(Map<String, Object?> left, Map<String, Object?> right) {
  if (left.length != right.length) return false;
  for (final entry in left.entries) {
    if (!right.containsKey(entry.key) || right[entry.key] != entry.value) {
      return false;
    }
  }
  return true;
}
