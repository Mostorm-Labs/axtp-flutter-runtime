import 'package:axtp_flutter/axtp_flutter.dart';
import 'package:test/test.dart';

class RecordingStreamSink implements StreamSink {
  final opened = <StreamInfo>[];
  final chunks = <StreamPayload>[];
  final closed = <StreamInfo>[];

  @override
  void onStreamOpened(StreamInfo info) {
    opened.add(info);
  }

  @override
  void onStreamChunk(StreamInfo info, StreamPayload stream) {
    chunks.add(stream);
  }

  @override
  void onStreamClosed(StreamInfo info) {
    closed.add(info);
  }
}

StreamPayload chunk(int streamId, int seqId, int cursor, int size) {
  return StreamPayload(
    streamId: streamId,
    seqId: seqId,
    cursor: cursor,
    data: List<int>.filled(size, seqId + 1),
  );
}

void main() {
  test('stream registry tracks lifecycle stats and sequence anomalies', () {
    final sink = RecordingStreamSink();
    final registry = StreamRegistry(streamSink: sink);
    const info = StreamInfo(
      streamId: 0x10,
      kind: 'file',
      source: 'firmware.bin',
      streamProfile: 'file.transfer',
      cursorUnit: 'offsetBytes',
      payloadFormat: 'binary',
      metadata: <String, Object?>{'sha256': 'abc'},
    );

    expect(
      registry.registerStream(info, rejectDuplicateKindSource: true),
      ErrorCode.success,
    );
    expect(registry.hasStream(0x10), isTrue);
    expect(registry.hasOpenStream('file', 'firmware.bin'), isTrue);
    expect(registry.activeStreamCount, 1);
    expect(sink.opened, <StreamInfo>[info]);

    expect(
      registry.registerStream(info, rejectDuplicateKindSource: true),
      ErrorCode.streamAlreadyOpen,
    );

    registry.handleStream(chunk(0x10, 0, 0, 3));
    registry.handleStream(chunk(0x10, 2, 3, 5));
    registry.handleStream(chunk(0x10, 2, 3, 7));
    registry.handleStream(chunk(0x99, 0, 0, 11));

    expect(registry.stats.chunks, 3);
    expect(registry.stats.bytes, 15);
    expect(registry.stats.seqGaps, 1);
    expect(registry.stats.duplicateSeq, 1);
    expect(registry.stats.unknownChunks, 1);
    expect(sink.chunks, hasLength(3));

    expect(registry.close(0x10), info);
    expect(sink.closed, <StreamInfo>[info]);
    expect(registry.activeStreamCount, 0);
    expect(registry.close(0x10), isNull);
  });
}
