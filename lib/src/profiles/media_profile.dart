import 'dart:convert';

import '../broker.dart';
import '../generated/axtp_registry_generated.dart';
import '../model.dart';
import '../stream.dart';

enum MediaKind { video, audio }

enum OpenMode { receiverPull, producerOpen, both }

bool receiverPullEnabled(OpenMode mode) =>
    mode == OpenMode.receiverPull || mode == OpenMode.both;

bool producerOpenEnabled(OpenMode mode) =>
    mode == OpenMode.producerOpen || mode == OpenMode.both;

class MediaStreamStats {
  const MediaStreamStats({
    this.videoChunks = 0,
    this.audioChunks = 0,
    this.videoBytes = 0,
    this.audioBytes = 0,
    this.unknownChunks = 0,
    this.seqGaps = 0,
    this.duplicateSeq = 0,
  });

  final int videoChunks;
  final int audioChunks;
  final int videoBytes;
  final int audioBytes;
  final int unknownChunks;
  final int seqGaps;
  final int duplicateSeq;

  MediaStreamStats copyWith({
    int? videoChunks,
    int? audioChunks,
    int? videoBytes,
    int? audioBytes,
    int? unknownChunks,
    int? seqGaps,
    int? duplicateSeq,
  }) {
    return MediaStreamStats(
      videoChunks: videoChunks ?? this.videoChunks,
      audioChunks: audioChunks ?? this.audioChunks,
      videoBytes: videoBytes ?? this.videoBytes,
      audioBytes: audioBytes ?? this.audioBytes,
      unknownChunks: unknownChunks ?? this.unknownChunks,
      seqGaps: seqGaps ?? this.seqGaps,
      duplicateSeq: duplicateSeq ?? this.duplicateSeq,
    );
  }
}

class MediaStreamInfo {
  const MediaStreamInfo({
    this.kind = MediaKind.video,
    this.streamId = 0,
    this.source = '',
    this.codec = '',
    this.streamProfile = '',
    this.cursorUnit = '',
    this.width = 0,
    this.height = 0,
    this.sampleRate = 0,
    this.channels = 0,
    this.metadata = const <String, Object?>{},
  });

  final MediaKind kind;
  final int streamId;
  final String source;
  final String codec;
  final String streamProfile;
  final String cursorUnit;
  final int width;
  final int height;
  final int sampleRate;
  final int channels;
  final Map<String, Object?> metadata;
}

class ActiveMediaStream {
  const ActiveMediaStream({
    this.kind = MediaKind.video,
    this.streamId = 0,
    this.source = '',
  });

  final MediaKind kind;
  final int streamId;
  final String source;
}

abstract interface class MediaStreamSink {
  void onStreamOpened(MediaStreamInfo info);
  void onStreamChunk(MediaKind kind, StreamPayload stream);
  void onStreamClosed(MediaKind kind, int streamId);
}

class OpenStreamResult {
  const OpenStreamResult({
    this.status = ErrorCode.success,
    this.body = const <String, Object?>{},
  });

  final ErrorCode status;
  final Map<String, Object?> body;
}

class MediaStreamRegistry implements StreamSink {
  MediaStreamRegistry({
    this.acceptVideo = true,
    this.acceptAudio = true,
    this.openMode = OpenMode.receiverPull,
    this.source = 'wireless_cast',
    this.audioFormat = 'adts',
    this.audioSampleRate = 48000,
    this.audioChannels = 1,
    this.streamSink,
  }) {
    _streams = StreamRegistry(streamSink: this);
  }

  final bool acceptVideo;
  final bool acceptAudio;
  final OpenMode openMode;
  final String source;
  final String audioFormat;
  final int audioSampleRate;
  final int audioChannels;
  final MediaStreamSink? streamSink;
  late final StreamRegistry _streams;
  MediaStreamStats _stats = const MediaStreamStats();
  int _nextVideoStreamId = 0x1001;
  int _nextAudioStreamId = 0x2001;

  bool get isReceiverPullEnabled => receiverPullEnabled(openMode);

  bool get isProducerOpenEnabled => producerOpenEnabled(openMode);

  bool mediaEnabled(MediaKind kind) =>
      kind == MediaKind.video ? acceptVideo : acceptAudio;

  String sourceFor(MediaKind kind) {
    if (source.isEmpty || source == 'wireless_cast') {
      return kind == MediaKind.video
          ? 'wireless_cast_video'
          : 'wireless_cast_audio';
    }
    return source;
  }

  bool hasOpenStream(MediaKind kind, String source) =>
      _streams.hasOpenStream(_kindName(kind), source);

  OpenStreamResult acceptProducerOpen(MediaKind kind, String paramsText) {
    if (!isProducerOpenEnabled) {
      return const OpenStreamResult(status: ErrorCode.rpcParamInvalid);
    }
    if (!mediaEnabled(kind)) {
      return const OpenStreamResult(status: ErrorCode.notSupported);
    }
    final params = _parseObject(paramsText);
    if (params == null) {
      return const OpenStreamResult(status: ErrorCode.rpcParamInvalid);
    }

    final source = _jsonStringOr(params, 'source', sourceFor(kind));
    final peerRole = _jsonStringOr(params, 'peerRole', 'receiver');
    final syncGroupId = _jsonStringOr(params, 'syncGroupId', '');
    final castSessionId = _jsonStringOr(params, 'castSessionId', '');
    final maxDataSize = _jsonU32Or(params, 'maxDataSize', 0);

    if (kind == MediaKind.video) {
      final codec = _jsonStringOr(params, 'codec', 'h264');
      if (codec != 'h264') {
        return const OpenStreamResult(status: ErrorCode.mediaCodecUnsupported);
      }
      return _openAccepted(
        kind,
        _allocateStreamId(kind),
        source,
        peerRole,
        'h264',
        'media.video',
        'timestampUs',
        syncGroupId,
        castSessionId,
        maxDataSize,
        <String, Object?>{
          'codecFormat': 'annexb',
          'parameterSetsInKeyFrame': true,
        },
      );
    }

    final codec = _jsonStringOr(params, 'codec', 'aac');
    if (codec != 'aac') {
      return const OpenStreamResult(status: ErrorCode.mediaCodecUnsupported);
    }
    final transportFormat =
        _jsonStringOr(params, 'transportFormat', audioFormat);
    if (transportFormat != 'adts') {
      return const OpenStreamResult(status: ErrorCode.mediaCodecUnsupported);
    }
    return _openAccepted(
      kind,
      _allocateStreamId(kind),
      source,
      peerRole,
      'aac',
      'media.audio',
      'timestampUs',
      syncGroupId,
      castSessionId,
      maxDataSize,
      <String, Object?>{
        'transportFormat': transportFormat,
        'sampleRate': _jsonU32Or(params, 'sampleRate', audioSampleRate),
        'channels': _jsonU32Or(params, 'channels', audioChannels),
      },
    );
  }

  OpenStreamResult registerPulledOpen(MediaKind kind, String responseText) {
    if (!mediaEnabled(kind)) {
      return const OpenStreamResult(status: ErrorCode.notSupported);
    }
    final result = _parseObject(responseText);
    if (result == null) {
      return const OpenStreamResult(status: ErrorCode.rpcPayloadInvalid);
    }
    final streamId = _jsonU32Or(result, 'streamId', 0);
    if (streamId == 0) {
      return const OpenStreamResult(status: ErrorCode.rpcPayloadInvalid);
    }
    final codec = _jsonStringOr(
      result,
      'codec',
      kind == MediaKind.video ? 'h264' : 'aac',
    );
    if (kind == MediaKind.video && codec != 'h264') {
      return const OpenStreamResult(status: ErrorCode.mediaCodecUnsupported);
    }
    if (kind == MediaKind.audio &&
        (codec != 'aac' ||
            _jsonStringOr(result, 'transportFormat', 'adts') != 'adts')) {
      return const OpenStreamResult(status: ErrorCode.mediaCodecUnsupported);
    }
    final extra = Map<String, Object?>.from(result);
    if (kind == MediaKind.audio) {
      extra.putIfAbsent('sampleRate', () => audioSampleRate == 0 ? 48000 : audioSampleRate);
      extra.putIfAbsent('channels', () => audioChannels == 0 ? 1 : audioChannels);
    }
    return _openAccepted(
      kind,
      streamId,
      _jsonStringOr(result, 'source', sourceFor(kind)),
      _jsonStringOr(result, 'peerRole', 'transmitter'),
      codec,
      _jsonStringOr(
        result,
        'streamProfile',
        kind == MediaKind.video ? 'media.video' : 'media.audio',
      ),
      _jsonStringOr(result, 'cursorUnit', 'timestampUs'),
      _jsonStringOr(result, 'syncGroupId', ''),
      _jsonStringOr(result, 'castSessionId', ''),
      _jsonU32Or(result, 'maxDataSize', 0),
      extra,
    );
  }

  OpenStreamResult close(MediaKind kind, String paramsText) {
    final params = _parseObject(paramsText);
    if (params == null) {
      return const OpenStreamResult(status: ErrorCode.rpcParamInvalid);
    }
    final streamId = _jsonU32Or(params, 'streamId', 0);
    if (streamId == 0) {
      return const OpenStreamResult(status: ErrorCode.rpcParamMissing);
    }
    var alreadyClosed = true;
    final info = _streams.findStream(streamId);
    if (info != null) {
      alreadyClosed = false;
      if (_kindFromStreamInfo(info) != kind) {
        return const OpenStreamResult(status: ErrorCode.streamNotFound);
      }
      _streams.close(streamId);
    }
    return OpenStreamResult(
      body: <String, Object?>{
        'streamId': streamId,
        'state': 'closed',
        'alreadyClosed': alreadyClosed,
      },
    );
  }

  OpenStreamResult closeLocal(MediaKind kind, int streamId) {
    return close(kind, jsonEncode(<String, Object?>{
      'streamId': streamId,
      'peerRole': 'transmitter',
    }));
  }

  void handleStream(StreamPayload stream) {
    _streams.handleStream(stream);
    final streamStats = _streams.stats;
    _stats = _stats.copyWith(
      unknownChunks: streamStats.unknownChunks,
      seqGaps: streamStats.seqGaps,
      duplicateSeq: streamStats.duplicateSeq,
    );
  }

  MediaStreamStats get stats {
    final streamStats = _streams.stats;
    return _stats.copyWith(
      unknownChunks: streamStats.unknownChunks,
      seqGaps: streamStats.seqGaps,
      duplicateSeq: streamStats.duplicateSeq,
    );
  }

  int get activeStreamCount => _streams.activeStreamCount;

  List<ActiveMediaStream> activeStreamsSnapshot() {
    return _streams
        .activeStreamsSnapshot()
        .map(
          (stream) => ActiveMediaStream(
            kind: _kindFromName(stream.kind),
            streamId: stream.streamId,
            source: stream.source,
          ),
        )
        .toList();
  }

  @override
  void onStreamOpened(StreamInfo info) {
    streamSink?.onStreamOpened(_toMediaInfo(info));
  }

  @override
  void onStreamChunk(StreamInfo info, StreamPayload stream) {
    final kind = _kindFromStreamInfo(info);
    if (kind == MediaKind.video) {
      _stats = _stats.copyWith(
        videoChunks: _stats.videoChunks + 1,
        videoBytes: _stats.videoBytes + stream.data.length,
      );
    } else {
      _stats = _stats.copyWith(
        audioChunks: _stats.audioChunks + 1,
        audioBytes: _stats.audioBytes + stream.data.length,
      );
    }
    streamSink?.onStreamChunk(kind, stream);
  }

  @override
  void onStreamClosed(StreamInfo info) {
    streamSink?.onStreamClosed(_kindFromStreamInfo(info), info.streamId);
  }

  OpenStreamResult _openAccepted(
    MediaKind kind,
    int streamId,
    String source,
    String peerRole,
    String codec,
    String streamProfile,
    String cursorUnit,
    String syncGroupId,
    String castSessionId,
    int maxDataSize,
    Map<String, Object?> extra,
  ) {
    final body = <String, Object?>{
      'streamId': streamId,
      'state': 'streaming',
      'source': source,
      'peerRole': peerRole,
      'codec': codec,
      'streamProfile': streamProfile,
      'cursorUnit': cursorUnit,
      ...extra,
    };
    if (syncGroupId.isNotEmpty) body['syncGroupId'] = syncGroupId;
    if (castSessionId.isNotEmpty) body['castSessionId'] = castSessionId;
    if (maxDataSize != 0) body['maxDataSize'] = maxDataSize;

    final status = _streams.registerStream(
      StreamInfo(
        streamId: streamId,
        kind: _kindName(kind),
        source: body['source'] as String? ?? '',
        payloadFormat: body['codec'] as String? ?? '',
        streamProfile: body['streamProfile'] as String? ?? '',
        cursorUnit: body['cursorUnit'] as String? ?? '',
        metadata: body,
      ),
      rejectDuplicateKindSource: true,
    );
    if (status != ErrorCode.success) {
      return OpenStreamResult(status: status);
    }
    return OpenStreamResult(body: body);
  }

  int _allocateStreamId(MediaKind kind) {
    if (kind == MediaKind.video) {
      return _nextVideoStreamId++;
    }
    return _nextAudioStreamId++;
  }
}

void installMediaHostHandlers(BasicBroker broker, MediaStreamRegistry registry) {
  RawRpcHandler handler(MediaKind kind, bool open) {
    return (context, request) {
      final result = open
          ? registry.acceptProducerOpen(kind, utf8.decode(request.body))
          : registry.close(kind, utf8.decode(request.body));
      return RpcResponseData(
        encoding: RpcEncoding.json,
        body: result.status == ErrorCode.success
            ? utf8.encode(jsonEncode(result.body))
            : const <int>[],
        overrideEncoding: true,
        statusCode: result.status,
        overrideStatus: true,
      );
    };
  }

  broker.registerRawMethod(MethodId.videoOpenStream.value, handler(MediaKind.video, true));
  broker.registerRawMethod(MethodId.audioOpenStream.value, handler(MediaKind.audio, true));
  broker.registerRawMethod(MethodId.videoCloseStream.value, handler(MediaKind.video, false));
  broker.registerRawMethod(MethodId.audioCloseStream.value, handler(MediaKind.audio, false));
  broker.registerStreamHandler((context, stream) {
    registry.handleStream(stream);
    return null;
  });
}

Map<String, Object?>? _parseObject(String text) {
  if (text.isEmpty) return <String, Object?>{};
  try {
    final parsed = jsonDecode(text);
    if (parsed is Map<String, Object?>) return parsed;
    if (parsed is Map) {
      return parsed.map((key, value) => MapEntry(key.toString(), value));
    }
  } catch (_) {
    return null;
  }
  return null;
}

String _jsonStringOr(Map<String, Object?> object, String name, String fallback) {
  final value = object[name];
  return value is String ? value : fallback;
}

int _jsonU32Or(Map<String, Object?> object, String name, int fallback) {
  final value = object[name];
  if (value is int && value >= 0 && value <= 0xffffffff) return value;
  return fallback;
}

String _kindName(MediaKind kind) => kind == MediaKind.video ? 'video' : 'audio';

MediaKind _kindFromName(String kind) =>
    kind == 'audio' ? MediaKind.audio : MediaKind.video;

MediaKind _kindFromStreamInfo(StreamInfo info) => _kindFromName(info.kind);

MediaStreamInfo _toMediaInfo(StreamInfo info) {
  return MediaStreamInfo(
    kind: _kindFromStreamInfo(info),
    streamId: info.streamId,
    source: info.source,
    codec: info.payloadFormat,
    streamProfile: info.streamProfile,
    cursorUnit: info.cursorUnit,
    width: _jsonU32Or(
      info.metadata,
      'width',
      _jsonU32Or(info.metadata, 'codedWidth', 0),
    ),
    height: _jsonU32Or(
      info.metadata,
      'height',
      _jsonU32Or(info.metadata, 'codedHeight', 0),
    ),
    sampleRate: _jsonU32Or(info.metadata, 'sampleRate', 0),
    channels: _jsonU32Or(info.metadata, 'channels', 0),
    metadata: Map<String, Object?>.from(info.metadata),
  );
}
