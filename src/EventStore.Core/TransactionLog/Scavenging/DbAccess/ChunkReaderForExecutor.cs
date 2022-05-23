using System.Collections.Generic;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkReaderForExecutor : IChunkReaderForExecutor<string, LogRecord> {
		private readonly TFChunk _chunk;

		public ChunkReaderForExecutor(TFChunk chunk) {
			_chunk = chunk;
		}

		public int ChunkStartNumber => _chunk.ChunkHeader.ChunkStartNumber;

		public int ChunkEndNumber => _chunk.ChunkHeader.ChunkEndNumber;

		public bool IsReadOnly => _chunk.IsReadOnly;

		public long ChunkEndPosition => _chunk.ChunkHeader.ChunkEndPosition;

		// similar to TFChunkScavenger.TraverseChunkBasic
		public IEnumerable<bool> ReadInto(
			RecordForExecutor<string, LogRecord>.NonPrepare nonPrepare,
			RecordForExecutor<string, LogRecord>.Prepare prepare) {

			var result = _chunk.TryReadFirst();
			while (result.Success) {
				var record = result.LogRecord;
				if (record.RecordType != LogRecordType.Prepare) {
					nonPrepare.SetRecord(record);
					yield return false;
				} else {
					var sourcePrepare = record as PrepareLogRecord;
					prepare.SetRecord(
						length: result.RecordLength,
						logPosition: record.LogPosition,
						record: record,
						timeStamp: sourcePrepare.TimeStamp,
						streamId: sourcePrepare.EventStreamId,
						eventNumber: sourcePrepare.ExpectedVersion + 1);
					yield return true;
				}

				result = _chunk.TryReadClosestForward(result.NextPosition);
			}
		}
	}
}
