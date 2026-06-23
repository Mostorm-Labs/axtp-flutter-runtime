import 'dart:convert';

import 'package:axtp_flutter/axtp_flutter.dart';
import 'package:test/test.dart';

class RecordingMediaSink implements MediaStreamSink {
  final opened = <MediaStreamInfo>[];
  final chunks = <StreamPayload>[];
  final closed = <({MediaKind kind, int streamId})>[];

  @override
  void onStreamOpened(MediaStreamInfo info) {
    opened.add(info);
  }

  @override
  void onStreamChunk(MediaKind kind, StreamPayload stream) {
    chunks.add(stream);
  }

  @override
  void onStreamClosed(MediaKind kind, int streamId) {
    closed.add((kind: kind, streamId: streamId));
  }
}

void main() {
  test('media profile opens producer video streams and routes chunks', () {
    final sink = RecordingMediaSink();
    final broker = BasicBroker();
    final registry = MediaStreamRegistry(
      openMode: OpenMode.producerOpen,
      streamSink: sink,
    );
    installMediaHostHandlers(broker, registry);

    broker.submit(
      BrokerTask(
        type: BrokerTaskType.rpcRequest,
        rpc: RpcPayload(
          encoding: RpcEncoding.json,
          op: RpcOp.request,
          requestId: 77,
          methodOrEventId: MethodId.videoOpenStream.value,
          bodyEncoding: RpcBodyEncoding.noneValue,
          meta: const PayloadMeta(
            sourceProtocol: SourceProtocol.jsonRpc,
            requestId: 77,
            jsonMethodOrEventName: 'video.openStream',
          ),
          body: utf8.encode(
            '{"source":"wireless_cast_video","peerRole":"receiver","codec":"h264"}',
          ),
        ),
      ),
    );
    broker.poll();

    final openResult = broker.pollResult()!.rpc;
    expect(openResult.statusCode, ErrorCode.success);
    final openBody = jsonDecode(utf8.decode(openResult.body))
        as Map<String, Object?>;
    expect(openBody['streamId'], 0x1001);
    expect(openBody['codec'], 'h264');
    expect(openBody['codecFormat'], 'annexb');
    expect(sink.opened.single.kind, MediaKind.video);

    broker.submit(
      BrokerTask(
        type: BrokerTaskType.streamData,
        stream: StreamPayload(
          streamId: 0x1001,
          seqId: 0,
          cursor: 1000,
          data: <int>[0x00, 0x00, 0x01, 0x67, 0x42],
        ),
      ),
    );
    broker.poll();
    expect(broker.pollResult(), isNull);
    expect(registry.stats.videoChunks, 1);
    expect(registry.stats.videoBytes, 5);
    expect(sink.chunks.single.streamId, 0x1001);

    broker.submit(
      BrokerTask(
        type: BrokerTaskType.rpcRequest,
        rpc: RpcPayload(
          encoding: RpcEncoding.json,
          op: RpcOp.request,
          requestId: 78,
          methodOrEventId: MethodId.videoCloseStream.value,
          bodyEncoding: RpcBodyEncoding.noneValue,
          body: utf8.encode('{"streamId":4097,"peerRole":"transmitter"}'),
        ),
      ),
    );
    broker.poll();

    final closeResult = broker.pollResult()!.rpc;
    expect(closeResult.statusCode, ErrorCode.success);
    expect(sink.closed, <({MediaKind kind, int streamId})>[
      (kind: MediaKind.video, streamId: 0x1001),
    ]);
    expect(registry.activeStreamCount, 0);
  });
}
