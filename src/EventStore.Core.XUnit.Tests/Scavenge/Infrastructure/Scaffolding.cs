using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	//qq the scaffold classes help us to get things tested before we have the real implementations
	// written, but will be removed once we can drop in the real implementations (which can run against
	// memdb for rapid testing)

	public class ScaffoldScavengePointSource : IScavengePointSource {
		private readonly int _chunkSize;
		private readonly LogRecord[][] _log;
		private readonly DateTime _effectiveNow;

		public ScaffoldScavengePointSource(
			int chunkSize,
			LogRecord[][] log,
			DateTime effectiveNow) {

			_chunkSize = chunkSize;
			_log = log;
			_effectiveNow = effectiveNow;
		}

		public Task<ScavengePoint> GetLatestScavengePointAsync() {
			ScavengePoint scavengePoint = default;
			foreach (var record in AllRecords()) {
				if (record is PrepareLogRecord prepare &&
					prepare.EventType == SystemEventTypes.ScavengePoint) {

					scavengePoint = ScavengePoint.CreateForLogPosition(
						chunkSize: _chunkSize,
						scavengePointLogPosition: record.LogPosition,
						eventNumber: prepare.ExpectedVersion + 1,
						effectiveNow: prepare.TimeStamp);
				}
			}

			return Task.FromResult(scavengePoint);
		}


		//qq maybe actually add it to the log, or not if we get rid of this soon enough
		public async Task<ScavengePoint> AddScavengePointAsync(long expectedVersion) {
			var latestScavengePoint = await GetLatestScavengePointAsync();
			var actualVersion = latestScavengePoint != null
				? latestScavengePoint.EventNumber
				: -1;

			if (actualVersion != expectedVersion) {
				//qq this probably isn't the right exception
				throw new InvalidOperationException(
					$"wrong version number {expectedVersion} vs {actualVersion}");
			}

			var lastChunk = _log.Length - 1;
			var newChunk = _log[lastChunk].ToList();
			newChunk.Add(LogRecord.SingleWrite(
				logPosition: lastChunk * _chunkSize + 20,
				correlationId: Guid.NewGuid(),
				eventId: Guid.NewGuid(),
				eventStreamId: SystemStreams.ScavengesStream,
				expectedVersion: expectedVersion, // no optimistic concurrency
				eventType: SystemEventTypes.ScavengePoint,
				data: LogRecord.NoData,
				metadata: null,
				timestamp: _effectiveNow));

			_log[_log.Length - 1] = newChunk.ToArray();

			var scavengePoint = await GetLatestScavengePointAsync();
			return scavengePoint;
		}

		private IEnumerable<LogRecord> AllRecords() {
			for (var chunkIndex = 0; chunkIndex < _log.Length; chunkIndex++) {
				for (var recordIndex = 0; recordIndex < _log[chunkIndex].Length; recordIndex++) {
					yield return _log[chunkIndex][recordIndex];
				}
			}
		}
	}

	public class ScaffoldChunkReaderForAccumulator : IChunkReaderForAccumulator<string> {
		private readonly LogRecord[][] _log;
		private readonly IMetastreamLookup<string> _metastreamLookup;

		public ScaffoldChunkReaderForAccumulator(
			LogRecord[][] log,
			IMetastreamLookup<string> metastreamLookup) {

			_log = log;
			_metastreamLookup = metastreamLookup;
		}

		public IEnumerable<RecordForAccumulator<string>> ReadChunk(int logicalChunkNumber) {
			if (logicalChunkNumber >= _log.Length) {
				throw new ArgumentOutOfRangeException(
					nameof(logicalChunkNumber),
					logicalChunkNumber,
					null);
			}

			foreach (var record in _log[logicalChunkNumber]) {
				if (!(record is PrepareLogRecord prepare))
					continue;

				if (prepare.Flags.HasAnyOf(PrepareFlags.StreamDelete)) {
					yield return new RecordForAccumulator<string>.TombStoneRecord {
						EventNumber = prepare.ExpectedVersion + 1,
						LogPosition = prepare.LogPosition,
						StreamId = prepare.EventStreamId,
						TimeStamp = prepare.TimeStamp,
					};
				} else if (_metastreamLookup.IsMetaStream(prepare.EventStreamId)) {
					yield return new RecordForAccumulator<string>.MetadataStreamRecord {
						EventNumber = prepare.ExpectedVersion + 1,
						LogPosition = prepare.LogPosition,
						Metadata = StreamMetadata.TryFromJsonBytes(prepare),
						StreamId = prepare.EventStreamId,
						TimeStamp = prepare.TimeStamp,
					};
				} else {
					yield return new RecordForAccumulator<string>.OriginalStreamRecord {
						LogPosition = prepare.LogPosition,
						StreamId = prepare.EventStreamId,
						TimeStamp = prepare.TimeStamp,
					};
				}
			}
		}
	}


	public class ScaffoldIndexForScavenge : IIndexReaderForCalculator<string> {
		private readonly LogRecord[][] _log;
		private readonly ILongHasher<string> _hasher;

		public ScaffoldIndexForScavenge(LogRecord[][] log, ILongHasher<string> hasher) {
			_log = log;
			_hasher = hasher;
		}

		public long GetLastEventNumber(StreamHandle<string> handle, ScavengePoint scavengePoint) {
			var lastEventNumber = -1L;
			//qq technically should only to consider committed prepares but probably doesn't matter
			// for our purposes here.
			var stopBefore = scavengePoint.UpToPosition;
			foreach (var chunk in _log) {
				foreach (var record in chunk) {
					if (record.LogPosition >= stopBefore)
						return lastEventNumber;

					if (!(record is PrepareLogRecord prepare))
						continue;

					switch (handle.Kind) {
						case StreamHandle.Kind.Hash:
							if (_hasher.Hash(prepare.EventStreamId) == handle.StreamHash) {
								lastEventNumber = prepare.ExpectedVersion + 1;
							}
							break;
						case StreamHandle.Kind.Id:
							if (prepare.EventStreamId == handle.StreamId) {
								lastEventNumber = prepare.ExpectedVersion + 1;
							}
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
					}
				}
			}

			return lastEventNumber;
		}

		public EventInfo[] ReadEventInfoForward(
			StreamHandle<string> handle,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			var result = new List<EventInfo>();

			var stopBefore = scavengePoint.UpToPosition;

			foreach (var chunk in _log) {
				foreach (var record in chunk) {
					if (record.LogPosition >= stopBefore)
						goto Done;

					if (result.Count >= maxCount)
						goto Done;

					if (!(record is PrepareLogRecord prepare))
						continue;

					if (prepare.ExpectedVersion + 1 < fromEventNumber)
						continue;

					switch (handle.Kind) {
						case StreamHandle.Kind.Hash:
							if (_hasher.Hash(prepare.EventStreamId) == handle.StreamHash)
								break;
							continue;
						case StreamHandle.Kind.Id:
							if (prepare.EventStreamId == handle.StreamId)
								break;
							continue;
						default:
							throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
					}

					result.Add(new EventInfo(prepare.LogPosition, prepare.ExpectedVersion + 1));
				}
			}

			Done:
			return result.ToArray();
		}
	}

	public class ScaffoldChunkReaderForExecutor : IChunkReaderForExecutor<string> {
		private readonly int _chunkSize;
		private readonly int _logicalChunkNumber;
		private readonly LogRecord[] _chunk;

		public ScaffoldChunkReaderForExecutor(
			int chunkSize,
			int logicalChunkNumber,
			LogRecord[] chunk) {
			_chunkSize = chunkSize;
			_logicalChunkNumber = logicalChunkNumber;
			_chunk = chunk;
		}

		public int ChunkStartNumber => _logicalChunkNumber;

		public int ChunkEndNumber => _logicalChunkNumber;

		public bool IsReadOnly => true;

		public long ChunkEndPosition => (_logicalChunkNumber + 1) * (long)_chunkSize;

		public IEnumerable<RecordForScavenge<string>> ReadRecords() {
			foreach (var record in _chunk) {
				//qq hopefully getting rid of this scaffolding before long, but
				// if not, consider efficiency of this, maybe reuse array, writer, etc.
				var bytes = new byte[1024];
				using (var stream = new MemoryStream(bytes))
				using (var binaryWriter = new BinaryWriter(stream)) {
					record.WriteTo(binaryWriter);

					if (record is PrepareLogRecord prepare) {
						//qq at a test to make sure we keep the system records
						yield return RecordForScavenge.CreateScavengeable(
							streamId: prepare.EventStreamId,
							timeStamp: prepare.TimeStamp,
							eventNumber: prepare.ExpectedVersion + 1,
							bytes: bytes);
					} else {
						yield return RecordForScavenge.CreateNonScavengeable<string>(bytes);
					}
				}
			}
		}
	}

	public class ScaffoldChunkWriterForExecutor : IChunkWriterForExecutor<string, ScaffoldChunk> {
		private readonly List<LogRecord> _writtenChunk = new List<LogRecord>();
		private readonly int _logicalChunkNumber;

		public ScaffoldChunkWriterForExecutor(int logicalChunkNumber) {
			_logicalChunkNumber = logicalChunkNumber;
		}

		public ScaffoldChunk WrittenChunk => new ScaffoldChunk(
			logicalChunkNumber: _logicalChunkNumber,
			records: _writtenChunk.ToArray());

		public void WriteRecord(RecordForScavenge<string> record) {
			using (var stream = new MemoryStream(record.RecordBytes))
			using (var binaryReader = new BinaryReader(stream)) {
				var logRecord = LogRecord.ReadFrom(binaryReader);
				_writtenChunk.Add(logRecord);
			}
		}
	}

	public class ScaffoldChunkManagerForScavenge : IChunkManagerForChunkExecutor<string, ScaffoldChunk> {
		private readonly int _chunkSize;
		private readonly LogRecord[][] _log;

		public ScaffoldChunkManagerForScavenge(int chunkSize, LogRecord[][] log) {
			_chunkSize = chunkSize;
			//qqqqq should we pass in the original log, or copy of log
			_log = log;
		}

		public IChunkWriterForExecutor<string, ScaffoldChunk> CreateChunkWriter(
			int chunkStartNumber,
			int chunkEndNumber) {

			if (chunkStartNumber != chunkEndNumber) {
				throw new NotSupportedException(
					"non-singular range of chunk numbers not supported by this implementation");
			}

			return new ScaffoldChunkWriterForExecutor(chunkStartNumber);
		}

		public IChunkReaderForExecutor<string> GetChunkReaderFor(long position) {
			var chunkNum = (int)(position / _chunkSize);
			return new ScaffoldChunkReaderForExecutor(_chunkSize, chunkNum, _log[chunkNum]);
		}

		public bool TrySwitchChunk(
			ScaffoldChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName) {

			_log[chunk.LogicalChunkNumber] = chunk.Records;
			newFileName = $"chunk{chunk.LogicalChunkNumber}";
			return true;
		}
	}

	public class ScaffoldChunk {
		public ScaffoldChunk(int logicalChunkNumber, LogRecord[] records) {
			LogicalChunkNumber = logicalChunkNumber;
			Records = records;
		}

		public int LogicalChunkNumber { get; }
		public LogRecord[] Records { get; }
	}

	//qq
	public class ScaffoldStuffForIndexExecutor : IIndexScavenger {
		private readonly LogRecord[][] _log;
		private readonly ILongHasher<string> _hasher;

		public ScaffoldStuffForIndexExecutor(
			LogRecord[][] log,
			ILongHasher<string> hasher) {

			// here the log represents the index, haven't bothered to build a separate index structure
			_log = log;
			_hasher = hasher;
		}

		public void ScavengeIndex(
			long scavengePoint,
			Func<IndexEntry, bool> shouldKeep,
			IIndexScavengerLog log,
			CancellationToken cancellationToken) {

			Scavenged = _log
				.Select(chunk => chunk
					.OfType<PrepareLogRecord>()
					.Where(prepare => {
						cancellationToken.ThrowIfCancellationRequested();
						var eventNumber = prepare.ExpectedVersion + 1;
						if (eventNumber < 0)
							throw new NotImplementedException("transaction handling in scaffold index.. hopefully wont have to implement this either since its temporary");

						var entry = new IndexEntry(
							stream: _hasher.Hash(prepare.EventStreamId),
							version: eventNumber,
							position: prepare.LogPosition);

						return shouldKeep(entry);
					})
					.ToArray())
				.ToArray();
		}

		public LogRecord[][] Scavenged { get; private set; }
	}

	public class ScaffoldChunkReaderForIndexExecutor : IChunkReaderForIndexExecutor<string> {
		private readonly LogRecord[][] _log;

		public ScaffoldChunkReaderForIndexExecutor(LogRecord[][] log) {
			_log = log;
		}

		public bool TryGetStreamId(long position, out string streamId) {
			foreach (var chunk in _log) {
				foreach (var record in chunk) {
					if (!(record is PrepareLogRecord prepare))
						continue;

					if (prepare.LogPosition != position)
						continue;

					streamId = prepare.EventStreamId;
					return true;
				}
			}

			streamId = default;
			return false;
		}
	}
}
