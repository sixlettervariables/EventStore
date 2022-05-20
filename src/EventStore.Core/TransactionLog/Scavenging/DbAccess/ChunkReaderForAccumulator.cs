using System;
using System.Collections.Generic;
using EventStore.Core.Helpers;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkReaderForAccumulator<TStreamId> : IChunkReaderForAccumulator<TStreamId> {
		private readonly TFChunkManager _manager;
		private readonly IMetastreamLookup<TStreamId> _metaStreamLookup;
		private readonly IStreamIdConverter<TStreamId> _streamIdConverter;
		private readonly ICheckpoint _replicationChk;
		private readonly Func<int, byte[]> _getBuffer;
		private readonly Action _releaseBuffer;
		private readonly ReusableObject<PrepareLogRecordView> _reusablePrepareView;

		public ChunkReaderForAccumulator(
			TFChunkManager manager,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IStreamIdConverter<TStreamId> streamIdConverter,
			ICheckpoint replicationChk) {
			_manager = manager;
			_metaStreamLookup = metastreamLookup;
			_streamIdConverter = streamIdConverter;
			_replicationChk = replicationChk;

			var reusableRecordBuffer = new ReusableBuffer(8192);

			_getBuffer = size => reusableRecordBuffer.AcquireAsByteArray(size);
			_releaseBuffer = () => reusableRecordBuffer.Release();
			_reusablePrepareView = ReusableObject.Create(new PrepareLogRecordView());
		}

		public IEnumerable<RecordForAccumulator<TStreamId>> ReadChunk(
			int logicalChunkNumber,
			ReusableObject<RecordForAccumulator<TStreamId>.OriginalStreamRecord> originalStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.MetadataStreamRecord> metadataStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.TombStoneRecord> tombStoneRecord) {
			var chunk = _manager.GetChunk(logicalChunkNumber);
			long globalStartPos = chunk.ChunkHeader.ChunkStartPosition;
			long localPos = 0L;

			var replicationChk = _replicationChk.ReadNonFlushed();
			while (replicationChk == -1 ||
			       globalStartPos + localPos <= replicationChk) {
				var result = chunk.TryReadClosestForwardRaw(localPos, _getBuffer);

				if (!result.Success)
					break;

				switch (result.RecordType) {
					case LogRecordType.Prepare:
						var prepareViewInitParams = new PrepareLogRecordViewInitParams(result.Record, result.Length, _reusablePrepareView.Release);
						var prepareView = _reusablePrepareView.Acquire(prepareViewInitParams);

						var streamId = _streamIdConverter.ToStreamId(prepareView.EventStreamId);
						var recordInitParams = new RecordForAccumulatorInitParams<TStreamId>(prepareView, streamId);

						if (prepareView.Flags.HasFlag(PrepareFlags.DeleteTombstone)) {
							yield return tombStoneRecord.Acquire(recordInitParams);
						} else if (_metaStreamLookup.IsMetaStream(streamId)) {
							yield return metadataStreamRecord.Acquire(recordInitParams);
						} else {
							yield return originalStreamRecord.Acquire(recordInitParams);
						}
						break;
					case LogRecordType.Commit:
						break;
					case LogRecordType.System:
						break;
					default:
						throw new ArgumentOutOfRangeException(nameof(result.RecordType),
							$"Unexpected log record type: {result.RecordType}");
				}

				localPos = result.NextPosition;
				_releaseBuffer();
			}
		}
	}
}
