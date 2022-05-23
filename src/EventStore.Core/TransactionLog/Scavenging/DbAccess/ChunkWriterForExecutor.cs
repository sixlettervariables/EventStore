using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkWriterForExecutor : IChunkWriterForExecutor<string, LogRecord> {
		const int BatchLength = 1000;
		private readonly ChunkManagerForExecutor _manager;
		private readonly TFChunk _outputChunk;
		private readonly List<List<PosMap>> _posMapss;
		private int _lastFlushedPage = -1;

		public ChunkWriterForExecutor(
			ChunkManagerForExecutor manager,
			TFChunkDbConfig dbConfig,
			IChunkReaderForExecutor<string, LogRecord> sourceChunk) {

			_manager = manager;

			_posMapss = new List<List<PosMap>>(capacity: BatchLength) {
				new List<PosMap>(capacity: BatchLength)
			};

			// from TFChunkScavenger.ScavengeChunk
			var tmpChunkPath = Path.Combine(dbConfig.Path, Guid.NewGuid() + ".scavenge.tmp");
			try {
				_outputChunk = TFChunk.CreateNew(tmpChunkPath,
					dbConfig.ChunkSize,
					sourceChunk.ChunkStartNumber,
					sourceChunk.ChunkEndNumber,
					isScavenged: true,
					inMem: dbConfig.InMemDb,
					unbuffered: dbConfig.Unbuffered,
					writethrough: dbConfig.WriteThrough,
					initialReaderCount: dbConfig.InitialReaderCount,
					reduceFileCachePressure: dbConfig.ReduceFileCachePressure);
			} catch (IOException exc) {
				//qq log Log.ErrorException(exc,
				//	"IOException during creating new chunk for scavenging purposes. Stopping scavenging process...");
				throw;
			}
		}

		public void WriteRecord(RecordForExecutor<string, LogRecord> record) {
			var posMap = TFChunkScavenger.WriteRecord(_outputChunk, record.Record);

			// add the posmap in memory so we can write it when we complete
			//qq check this
			var last = _posMapss[_posMapss.Count - 1];
			if (last.Count >= BatchLength) {
				last = new List<PosMap>(capacity: BatchLength);
				_posMapss.Add(last);
			}

			last.Add(posMap);

			//qqqqq ocasionaly flushes the chunk, check this
			// based on TFChunkScavenger.ScavengeChunk
			var currentPage = _outputChunk.RawWriterPosition / 4046;
			if (currentPage - _lastFlushedPage > TFChunkScavenger.FlushPageInterval) {
				_outputChunk.Flush();
				_lastFlushedPage = currentPage;
			}
		}

		public void SwitchIn(out string newFileName) {
			// write posmap
			var posMapCount = 0;
			foreach (var list in _posMapss)
				posMapCount += list.Count;

			var unifiedPosMap = new List<PosMap>(capacity: posMapCount);
			foreach (var list in _posMapss)
				unifiedPosMap.AddRange(list);

			_outputChunk.CompleteScavenge(unifiedPosMap);

			_manager.SwitchChunk(chunk: _outputChunk, out newFileName);
		}
	}
}
