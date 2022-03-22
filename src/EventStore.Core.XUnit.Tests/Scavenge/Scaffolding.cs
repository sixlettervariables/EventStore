using System;
using System.Collections.Generic;
using System.IO;
using EventStore.Core.Data;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Services;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	//qq the scaffold classes help us to get things tested before we have the real implementations
	// written, but will be removed once we can drop in the real implementations (which can run against
	// memdb for rapid testing)
	public class ScaffoldHashUsageChecker : IHashUsageChecker<string> {
		private readonly ILongHasher<string> _hasher;
		private readonly LogRecord[][] _log;

		public ScaffoldHashUsageChecker(
			ILongHasher<string> hasher, LogRecord[][] log) {

			_hasher = hasher;
			_log = log;
		}

		public bool HashInUseBefore(ulong hash, long position, out string hashUser) {
			// iterate through the log

			foreach (var chunk in _log) {
				foreach (var record in chunk) {
					if (record.LogPosition >= position) {
						hashUser = default;
						return false;
					}

					switch (record) {
						//qq technically probably only have to detect in use if its committed
						// but this is a detail that probalby wont matter for us
						case PrepareLogRecord prepare: {
								//qq do these have populated event numbres? what about when committed?
							var stream = prepare.EventStreamId;
							if (_hasher.Hash(stream) == hash) {
								hashUser = stream;
								return true;
							}
							break;
						}
						//qq any other record types use streams?
						default:
							break;
					}
				}
			}

			hashUser = default;
			return false;
		}
	}

	public class ScaffoldScavengePointSource : IScavengePointSource {
		private readonly ScavengePoint _scavengePoint;

		public ScaffoldScavengePointSource(ScavengePoint scavengePoint) {
			_scavengePoint = scavengePoint;
		}

		public ScavengePoint GetScavengePoint() {
			return _scavengePoint;
		}
	}

	public class ScaffoldChunkReaderForAccumulator : IChunkReaderForAccumulator<string> {
		private readonly LogRecord[][] _log;

		public ScaffoldChunkReaderForAccumulator(LogRecord[][] log) {
			_log = log;
		}

		public IEnumerable<RecordForAccumulator<string>> Read(
			int startFromChunk,
			ScavengePoint scavengePoint) {

			var stopBefore = scavengePoint.Position;

			for (int chunkIndex = startFromChunk; chunkIndex < _log.Length; chunkIndex++) {
				var chunk = _log[chunkIndex];
				foreach (var record in chunk) {
					if (record.LogPosition >= stopBefore)
						yield break;

					if (!(record is PrepareLogRecord prepare))
						continue;

					//qq in each case what is the sufficient condition
					// do we worry about whether a user might have created system events
					// in the wrong place, or with the wrong event number, etc.

					if (prepare.EventType == SystemEventTypes.StreamMetadata) {
						yield return new RecordForAccumulator<string>.MetadataRecord {
							EventNumber = prepare.ExpectedVersion + 1,
							LogPosition = prepare.LogPosition,
							Metadata = StreamMetadata.FromJsonBytes(prepare.Data),
							StreamId = prepare.EventStreamId,
						};
					} else if (prepare.Flags.HasAnyOf(PrepareFlags.StreamDelete)) {
						yield return new RecordForAccumulator<string>.TombStoneRecord {
							EventNumber = prepare.ExpectedVersion + 1,
							LogPosition = prepare.LogPosition,
							StreamId = prepare.EventStreamId,
						};
					} else {
						yield return new RecordForAccumulator<string>.EventRecord {
							LogPosition = prepare.LogPosition,
							StreamId = prepare.EventStreamId,
						};
					}
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

		public long GetLastEventNumber(StreamHandle<string> streamHandle, ScavengePoint scavengePoint) {
			var lastEventNumber = -1L;
			//qq technically should only to consider committed prepares but probably doesn't matter
			// for our purposes here.
			var stopBefore = scavengePoint.Position;
			foreach (var chunk in _log) {
				foreach (var record in chunk) {
					if (record.LogPosition >= stopBefore)
						return lastEventNumber;

					if (!(record is PrepareLogRecord prepare))
						continue;

					if (streamHandle.IsHash) {
						if (_hasher.Hash(prepare.EventStreamId) == streamHandle.StreamHash) {
							lastEventNumber = prepare.ExpectedVersion + 1;
						}
					} else {
						if (prepare.EventStreamId == streamHandle.StreamId) {
							lastEventNumber = prepare.ExpectedVersion + 1;
						}
					}
				}
			}

			return lastEventNumber;
		}

		public EventInfo[] ReadEventInfoForward(
			StreamHandle<string> stream,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			var result = new List<EventInfo>();

			var stopBefore = scavengePoint.Position;

			foreach (var chunk in _log) {
				foreach (var record in chunk) {
					if (record.LogPosition >= stopBefore)
						goto Done;

					if (!(record is PrepareLogRecord prepare))
						continue;

					//qqqqqqqqq filter according to stream
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

		public IEnumerable<int> LogicalChunkNumbers {
			get {
				yield return _logicalChunkNumber;
			}
		}

		public IEnumerable<RecordForScavenge<string>> ReadRecords() {
			foreach (var record in _chunk) {
				if (!(record is PrepareLogRecord prepare))
					continue;

				//qq hopefully getting rid of this scaffolding before long, but
				// if not, consider efficiency of this, maybe reuse array, writer, etc.
				var bytes = new byte[1024];
				using (var stream = new MemoryStream(bytes))
				using (var binaryWriter = new BinaryWriter(stream)) {
					record.WriteTo(binaryWriter);
					yield return new RecordForScavenge<string>() {
						StreamId = prepare.EventStreamId,
						EventNumber = prepare.ExpectedVersion + 1,
						RecordBytes = bytes,
					};
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
	public class ScaffoldStuffForIndexExecutor : IDoStuffForIndexExecutor {
		public ScaffoldStuffForIndexExecutor(LogRecord[][] log) {
		}
	}
}
