using System;
using System.Collections.Generic;
using System.Threading;
using EventStore.Core.Data;
using EventStore.Core.Helpers;
using EventStore.Core.Index;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq own files for stuff in here, to be placed in DbAccess
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

	public class ChunkBulkReaderForAccumulator<TStreamId> : IChunkReaderForAccumulator<TStreamId> {
		public IEnumerable<RecordForAccumulator<TStreamId>> ReadChunk(int logicalChunkNumber, ReusableObject<RecordForAccumulator<TStreamId>.OriginalStreamRecord> originalStreamRecord, ReusableObject<RecordForAccumulator<TStreamId>.MetadataStreamRecord> metadataStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.TombStoneRecord> tombStoneRecord) {
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
			return true;
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

		public IEnumerable<RecordForScavenge<string>> ReadRecords() {
			yield return new RecordForScavenge<string>() {
				StreamId = "thestream",
				TimeStamp = DateTime.UtcNow,
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

		public void SwitchChunk(
			TFChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName) {

			var tfChunk = _manager.SwitchChunk(
				chunk,
				verifyHash,
				removeChunksWithGreaterNumbers);

			if (tfChunk == null) {
				throw new Exception(); //qq detail, old scavenge handles this but it looks like its impossible
			}

			newFileName = tfChunk.FileName;
		}
	}

	public class IndexScavenger : IIndexScavenger {
		private readonly ITableIndex _tableIndex;

		public IndexScavenger(ITableIndex tableIndex) {
			_tableIndex = tableIndex;
		}

		public void ScavengeIndex(
			long scavengePoint,
			Func<IndexEntry, bool> shouldKeep,
			IIndexScavengerLog log,
			CancellationToken cancellationToken) {

			_tableIndex.Scavenge(shouldKeep, log, cancellationToken);
		}
	}

	public class ChunkReaderForIndexExecutor : IChunkReaderForIndexExecutor<string> {
		private readonly Func<TFReaderLease> _tfReaderFactory;

		//qq might want to hold the reader for longer than one operation.
		// but this is only called when the hash for htis position is a collision, so rare enough that
		// it probably doesn't matter.
		public ChunkReaderForIndexExecutor(Func<TFReaderLease> tfReaderFactory) {
			_tfReaderFactory = tfReaderFactory;
		}

		public bool TryGetStreamId(long position, out string streamId) {
			using (var reader = _tfReaderFactory()) {
				var result = reader.TryReadAt(position);
				if (!result.Success ||
					!(result.LogRecord is PrepareLogRecord prepare)) {

					streamId = default;
					return false;
				}

				streamId = prepare.EventStreamId;
				return true;
			}
		}
	}

	public class IndexReaderForAccumulator<TStreamId> : IIndexReaderForAccumulator<TStreamId> {
		public EventInfo[] ReadEventInfoBackward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			throw new NotImplementedException();
		}

		public EventInfo[] ReadEventInfoForward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			throw new NotImplementedException();
		}
	}
}
