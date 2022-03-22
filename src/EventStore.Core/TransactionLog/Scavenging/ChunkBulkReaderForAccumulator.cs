using System;
using System.Collections.Generic;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq own files for stuff in here
	public class ChunkReaderForAccumulator<TStreamId> : IChunkReaderForAccumulator<TStreamId> {
		public IEnumerable<RecordForAccumulator<TStreamId>> Read(int startFromChunk, ScavengePoint scavengePoint) {
			throw new NotImplementedException();
		}
	}

	public class ChunkBulkReaderForAccumulator<TStreamId> : IChunkReaderForAccumulator<TStreamId> {
		public IEnumerable<RecordForAccumulator<TStreamId>> Read(int startFromChunk, ScavengePoint scavengePoint) {
			throw new NotImplementedException();
		}
		/*
			//qq once we have index only streams, it could be interesting to have one of those and read that
			// rather than reading the chunks. or it could be a projection
			// or could something like an index-only stream do something like keep the last event
			// per stream for a subset of streams

					// for now just naive iterate through. if we stick with this we want to be able to
			// cancel it mid chunk. (old TraverseChunkBasic does this)
			IEnumerable<TFChunk> GetAllChunks(int startFromChunk) {
				var scavengePosition = db.Config.ChunkSize * (long)startFromChunk;
				var scavengePoint = db.Config.ChaserCheckpoint.Read();
				while (scavengePosition < scavengePoint) {
					var chunk = db.Manager.GetChunkFor(scavengePosition);
					if (!chunk.IsReadOnly) {
						yield break;
					}

					yield return chunk;

					scavengePosition = chunk.ChunkHeader.ChunkEndPosition;
				}
			}

			void TraverseChunkBasic(TFChunk chunk) {
				var result = chunk.TryReadFirst();
				var singletonArray = new IPrepareLogRecord<TStreamId>[1]; //qq stackalloc if we keep this
				while (result.Success) {
					//qq do we need to check if this is committed...
					// can you change metadata in a transaction?
					// can you tombstone in a transaction?
					// this probably doesn't matter if we switch this to continuous.
					//qq letting the accumulator decide what to keep
					//qq should we batch them togther or is presenting one at a time fine
					if (result.LogRecord is IPrepareLogRecord<TStreamId> prepare) {
						singletonArray[0] = prepare;
						accumulator.Confirm(
							replicatedPrepares: singletonArray,
							catchingUp: false, //qq hmm
							backend: null); //qq hmm
					}

					result = chunk.TryReadClosestForward(result.NextPosition);
				}
			}


			//qq refactor with GetStreamMetadataUncached
			var metadata = StreamMetadata.TryFromJsonBytes(prepare);
			var newData = data with {
				MaxAge = metadata.MaxAge,
				MaxCount = metadata.MaxCount,
				TruncateBefore = metadata.TruncateBefore,
			};

		private static bool IsMetadata(IPrepareLogRecord<TStreamId> prepare) {
			// prepare.EventType comparer equals metadataeventtype
			return true; //qqqq
		}

		private static bool IsTombStone(IPrepareLogRecord<TStreamId> prepare) {
			return prepare.ExpectedVersion + 1 == long.MaxValue;
		}


		 */
	}

	//qq implement
	public class ChunkReaderForExecutor : IChunkReaderForExecutor<string> {
		private readonly TFChunk _chunk;

		public ChunkReaderForExecutor(TFChunk chunk) {
			_chunk = chunk;
		}

		public int ChunkStartNumber => _chunk.ChunkHeader.ChunkStartNumber;

		public int ChunkEndNumber => _chunk.ChunkHeader.ChunkEndNumber;

		public bool IsReadOnly => _chunk.IsReadOnly;

		public long ChunkEndPosition => _chunk.ChunkHeader.ChunkEndPosition;

		public IEnumerable<int> LogicalChunkNumbers {
			get {
				for (var i = _chunk.ChunkHeader.ChunkStartNumber;
					i < _chunk.ChunkHeader.ChunkEndNumber;
					i++)

					yield return i;
			}
		}

		public IEnumerable<RecordForScavenge<string>> ReadRecords() {
			yield return new RecordForScavenge<string>() {
				StreamId = "thestream",
				EventNumber = 123,
				RecordBytes = null,
			};
		}
	}

	public class ChunkWriterForExecutor : IChunkWriterForExecutor<string, TFChunk> {
		private readonly TFChunk _tfChunk;

		public ChunkWriterForExecutor(TFChunkDbConfig dbConfig) {
			_tfChunk = null;
			//qq _tfChunk = TFChunk.CreateNew(...)
		}

		public TFChunk WrittenChunk => _tfChunk;

		public void WriteRecord(RecordForScavenge<string> record) {
			//qq _tfChunk.TryAppend(...)
			throw new NotImplementedException();
		}
	}

	//qq implement
	public class ChunkManagerForScavenge : IChunkManagerForChunkExecutor<string, TFChunk> {
		private readonly TFChunkManager _manager;
		private readonly TFChunkDbConfig _dbConfig;

		public ChunkManagerForScavenge(TFChunkManager manager, TFChunkDbConfig dbConfig) {
			_manager = manager;
			_dbConfig = dbConfig;
		}

		public IChunkWriterForExecutor<string, TFChunk> CreateChunkWriter(
			int chunkStartNumber, //qq pass along
			int chunkEndNumber) {

			return new ChunkWriterForExecutor(_dbConfig);
		}

		public IChunkReaderForExecutor<string> GetChunkReaderFor(long position) {
			var tfChunk = _manager.GetChunkFor(position);
			return new ChunkReaderForExecutor(tfChunk);
		}

		public bool TrySwitchChunk(
			TFChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName) {

			var tfChunk = _manager.SwitchChunk(
				chunk,
				verifyHash,
				removeChunksWithGreaterNumbers);

			if (tfChunk == null) {
				newFileName = default;
				return false;
			} else {
				newFileName = tfChunk.FileName;
				return true;
			}
		}
	}

	//qq
	//public class ChunkBulkReaderForScavenge<TStreamId> : IChunkReaderForChunkExecutor<TStreamId> {
	//	public IEnumerable<RecordForScavenge<TStreamId>> Read(TFChunk chunk) {
	//		throw new NotImplementedException();
	//		//using var reader = chunk.AcquireReader();
	//		//var start = 0;
	//		//var count = ChunkHeader.Size + chunk.ChunkFooter.PhysicalDataSize;

	//		//yield return new RecordForScavenge(); //qq
	//	}
	//}

	//qq
	public class StuffForIndexExecutor : IDoStuffForIndexExecutor {
	}
}
