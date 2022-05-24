using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogV2;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Settings;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.Util;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class Scenario {
		private Func<TFChunkDbConfig, DbResult> _getDb;
		private Func<ScavengeStateBuilder, ScavengeStateBuilder> _stateTransform;
		private List<ScavengePoint> _newScavengePoint;

		private bool _mergeChunks;
		private string _dbPath;
		private string _accumulatingCancellationTrigger;
		private string _calculatingCancellationTrigger;
		private string _executingChunkCancellationTrigger;
		private string _executingIndexEntryCancellationTrigger;
		private Type _cancelWhenCheckpointingType;
		private (string Message, int Line)[] _expectedTrace;
		private bool _unsafeIgnoreHardDeletes;

		protected Tracer Tracer { get; set; }

		public Scenario() {
			_getDb = dbConfig => throw new Exception("db not configured. call WithDb");
			_stateTransform = x => x;
			Tracer = new Tracer();
		}

		public Scenario WithTracerFrom(Scenario scenario) {
			Tracer = scenario.Tracer;
			return this;
		}

		public Scenario WithUnsafeIgnoreHardDeletes(bool unsafeIgnoreHardDeletes = true) {
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
			return this;
		}

		public Scenario WithDbPath(string path) {
			_dbPath = path;
			return this;
		}

		public Scenario WithDb(DbResult db) {
			_getDb = _ => db;
			return this;
		}

		public Scenario WithDb(Func<TFChunkDbCreationHelper, TFChunkDbCreationHelper> f) {
			_getDb = dbConfig => f(new TFChunkDbCreationHelper(dbConfig)).CreateDb();
			return this;
		}

		public Scenario WithState(Func<ScavengeStateBuilder, ScavengeStateBuilder> f) {
			var wrapped = _stateTransform;
			_stateTransform = builder => builder
				.TransformBuilder(wrapped)
				.TransformBuilder(f);
			return this;
		}

		public Scenario MutateState(Action<ScavengeState<string>> f) {
			var wrapped = _stateTransform;
			_stateTransform = builder => builder
				.TransformBuilder(wrapped)
				.MutateState(f);
			return this;
		}

		public Scenario WithMergeChunks(bool mergeChunks = true) {
			_mergeChunks = mergeChunks;
			return this;
		}

		public Scenario CancelOnNewScavengePoint(List<ScavengePoint> newScavengePoint) {
			_newScavengePoint = newScavengePoint;
			return this;
		}

		public Scenario CancelWhenAccumulatingMetaRecordFor(string trigger) {
			_accumulatingCancellationTrigger = trigger;
			return this;
		}

		// note for this to work the trigger stream needs metadata so it will be calculated
		// and it needs to have at least one record
		public Scenario CancelWhenCalculatingOriginalStream(string trigger) {
			_calculatingCancellationTrigger = trigger;
			return this;
		}

		public Scenario CancelWhenExecutingChunk(string trigger) {
			_executingChunkCancellationTrigger = trigger;
			return this;
		}

		public Scenario CancelWhenExecutingIndexEntry(string trigger) {
			_executingIndexEntryCancellationTrigger = trigger;
			return this;
		}

		public Scenario CancelWhenCheckpointing<TCheckpoint>() {
			_cancelWhenCheckpointingType = typeof(TCheckpoint);
			return this;
		}

		// Assert methods can be used to input checks that are internal to the scavenge
		// This is not black box testing, handle with care.
		public delegate Scenario TraceDelegate(params string[] expected);

		public Scenario AssertTrace(params (string, int)[] expected) {
			_expectedTrace = expected;
			return this;
		}

		public async Task<(ScavengeState<string>, DbResult)> RunAsync(
			Func<DbResult, LogRecord[][]> getExpectedKeptRecords = null,
			Func<DbResult, LogRecord[][]> getExpectedKeptIndexEntries = null) {

			return await RunInternalAsync(
				getExpectedKeptRecords,
				getExpectedKeptIndexEntries);
		}

		//qqq make sure the directory gets cleaned up
		// simialarities with ScavengeTestScenario
		private async Task<(ScavengeState<string>, DbResult)> RunInternalAsync(
			Func<DbResult, LogRecord[][]> getExpectedKeptRecords,
			Func<DbResult, LogRecord[][]> getExpectedKeptIndexEntries) {

			//qq use directory fixture, or memdb. pattern in ScavengeTestScenario.cs
			if (string.IsNullOrEmpty(_dbPath))
				throw new Exception("call WithDbPath");

			var dbConfig = TFChunkHelper.CreateDbConfig(_dbPath, 0, chunkSize: 1024 * 1024, memDb: true);
			var dbResult = _getDb(dbConfig);
			var keptRecords = getExpectedKeptRecords != null
				? getExpectedKeptRecords(dbResult)
				: null;

			var keptIndexEntries = getExpectedKeptIndexEntries != null
				? getExpectedKeptIndexEntries(dbResult)
				: keptRecords;

			dbResult.Db.Config.WriterCheckpoint.Flush();
			dbResult.Db.Config.ChaserCheckpoint.Write(dbResult.Db.Config.WriterCheckpoint.Read());
			dbResult.Db.Config.ChaserCheckpoint.Flush();

			var indexPath = Path.Combine(_dbPath, "index");
			var readerPool = new ObjectPool<ITransactionFileReader>(
				objectPoolName: "ReadIndex readers pool",
				initialCount: ESConsts.PTableInitialReaderCount,
				maxCount: ESConsts.PTableMaxReaderCount,
				factory: () => new TFChunkReader(dbResult.Db, dbResult.Db.Config.WriterCheckpoint));

			IHasher lowHasher = new XXHashUnsafe();
			IHasher highHasher = new Murmur3AUnsafe();
			ILongHasher<string> hasher = new CompositeHasher<string>(
				new XXHashUnsafe(),
				new Murmur3AUnsafe());

			//qqq configurable?
			var humanHashers = true;
			if (humanHashers) {
				lowHasher = new ConstantHasher(0);
				highHasher = new HumanReadableHasher32();
				hasher = new CompositeHasher<string>(new ConstantHasher(0), new HumanReadableHasher32());
			}

			var tableIndex = new TableIndex(
				directory: indexPath,
				lowHasher: lowHasher,
				highHasher: highHasher,
				memTableFactory: () => new HashListMemTable(PTableVersions.IndexV3, maxSize: 200),
				tfReaderFactory: () => new TFReaderLease(readerPool),
				ptableVersion: PTableVersions.IndexV3,
				maxAutoMergeIndexLevel: 5,
				maxSizeForMemory: 100,
				maxTablesPerLevel: 2);

			IReadIndex readIndex = new ReadIndex(
				bus: new NoopPublisher(),
				readerPool: readerPool,
				tableIndex: tableIndex,
				streamInfoCacheCapacity: 100,
				additionalCommitChecks: true,
				metastreamMaxCount: 1,
				hashCollisionReadLimit: Opts.HashCollisionReadLimitDefault,
				skipIndexScanOnReads: Opts.SkipIndexScanOnReadsDefault,
				replicationCheckpoint: dbResult.Db.Config.ReplicationCheckpoint);

			readIndex.Init(dbResult.Db.Config.WriterCheckpoint.Read());



			//qqqqqqqqqqqqqq we should call these before tearing down?
			//readIndex.Close();
			//dbResult.Db.Close();

			//qqqqqqqqqqqqq
			//// the log. will mutate as we scavenge.
			//var log = dbResult.Recs;

			//// original log. will not mutate, for calculating expected results.
			//var originalLog = log.ToArray();

			var cancellationTokenSource = new CancellationTokenSource();
			var metastreamLookup = new LogV2SystemStreams();

			IChunkReaderForAccumulator<string> chunkReader = new ChunkReaderForAccumulator<string>(
				dbResult.Db.Manager,
				metastreamLookup,
				new LogV2StreamIdConverter(),
				dbResult.Db.Config.ReplicationCheckpoint);

			var indexReader = new ScaffoldCheatingIndexReaderForAccumulator();

			var accumulatorMetastreamLookup = new AdHocMetastreamLookupInterceptor<string>(
				metastreamLookup,
				(continuation, streamId) => {
					if (streamId == _accumulatingCancellationTrigger)
						cancellationTokenSource.Cancel();
					return continuation(streamId);
				});

			var calculatorIndexReader = new AdHocIndexReaderInterceptor<string>(
				new IndexReaderForCalculator(readIndex),
				(f, handle, x) => {
					if (_calculatingCancellationTrigger != null &&
						handle.Kind == StreamHandle.Kind.Hash &&
						handle.StreamHash == hasher.Hash(_calculatingCancellationTrigger)) {

						cancellationTokenSource.Cancel();
					}
					return f(handle, x);
				});

			var chunkExecutorMetastreamLookup = new AdHocMetastreamLookupInterceptor<string>(
				metastreamLookup,
				(continuation, streamId) => {
					if (streamId == _executingChunkCancellationTrigger)
						cancellationTokenSource.Cancel();
					return continuation(streamId);
				});

			var indexScavenger = new IndexScavenger(tableIndex);
			var cancellationWrappedIndexScavenger = new AdHocIndexScavengerInterceptor(
				indexScavenger,
				f => entry => {
					if (_executingIndexEntryCancellationTrigger != null &&
						entry.Stream == hasher.Hash(_executingIndexEntryCancellationTrigger)) {

						cancellationTokenSource.Cancel();
					}
					return f(entry);
				});

			var cancellationCheckPeriod = 1;
			var checkpointPeriod = 2;

			// add tracing
			chunkReader = new TracingChunkReaderForAccumulator<string>(chunkReader, Tracer.Trace);

			IAccumulator<string> accumulator = new Accumulator<string>(
				chunkSize: dbConfig.ChunkSize,
				metastreamLookup: accumulatorMetastreamLookup,
				chunkReader: chunkReader,
				index: indexReader,
				cancellationCheckPeriod: cancellationCheckPeriod);

			ICalculator<string> calculator = new Calculator<string>(
				index: calculatorIndexReader,
				chunkSize: dbConfig.ChunkSize,
				cancellationCheckPeriod: cancellationCheckPeriod,
				checkpointPeriod: checkpointPeriod);

			IChunkExecutor<string> chunkExecutor = new ChunkExecutor<string, LogRecord>(
				metastreamLookup: chunkExecutorMetastreamLookup,
				chunkManager: new TracingChunkManagerForChunkExecutor<string, LogRecord>(
					new ChunkManagerForExecutor(
						dbResult.Db.Manager,
						dbConfig),
					Tracer),
				chunkSize: dbConfig.ChunkSize,
				unsafeIgnoreHardDeletes: _unsafeIgnoreHardDeletes,
				cancellationCheckPeriod: cancellationCheckPeriod);

			IChunkMerger chunkMerger = new ChunkMerger(
				mergeChunks: _mergeChunks,
				new OldScavengeChunkMergerBackend(dbResult.Db));

			IIndexExecutor<string> indexExecutor = new IndexExecutor<string>(
				indexScavenger: cancellationWrappedIndexScavenger,
				streamLookup: new ChunkReaderForIndexExecutor(() => new TFReaderLease(readerPool)),
				unsafeIgnoreHardDeletes: _unsafeIgnoreHardDeletes);

			ICleaner cleaner = new Cleaner(unsafeIgnoreHardDeletes: _unsafeIgnoreHardDeletes);

			accumulator = new TracingAccumulator<string>(accumulator, Tracer);
			calculator = new TracingCalculator<string>(calculator, Tracer);
			chunkExecutor = new TracingChunkExecutor<string>(chunkExecutor, Tracer);
			chunkMerger = new TracingChunkMerger(chunkMerger, Tracer);
			indexExecutor = new TracingIndexExecutor<string>(indexExecutor, Tracer);
			cleaner = new TracingCleaner(cleaner, Tracer);

			var scavengeState = new ScavengeStateBuilder(hasher, metastreamLookup)
				.TransformBuilder(_stateTransform)
				.CancelWhenCheckpointing(_cancelWhenCheckpointingType, cancellationTokenSource)
				.WithTracer(Tracer)
				.Build();

			var sut = new Scavenger<string>(
				scavengeState,
				accumulator,
				calculator,
				chunkExecutor,
				chunkMerger,
				indexExecutor,
				cleaner,
				new ScaffoldScavengePointSource2(
					dbResult,
					EffectiveNow,
					_newScavengePoint ?? new List<ScavengePoint>()),
				new FakeTFScavengerLog());

			Tracer.Reset();
			await sut.ScavengeAsync(cancellationTokenSource.Token);

			// check the trace
			if (_expectedTrace != null) {
				var expected = _expectedTrace;
				var actual = Tracer.ToArray();

				for (var i = 0; i < Math.Max(expected.Length, actual.Length); i++) {

					if (expected[i] == Tracer.AnythingElse) {
						// actual can be anything it likes from this point on
						break;
					}

					var line = expected[i].Line;
					Assert.True(
						i < expected.Length,
						i < actual.Length
							? $"Actual trace contains extra entries starting with: {actual[i]}"
							: "impossible");

					Assert.True(
						i < actual.Length,
						$"Expected trace contains extra entries starting from line {line}: {expected[i].Message}");

					Assert.True(
						expected[i].Message == actual[i],
						$"Trace mismatch at line {line}. \r\n" +
						$" Expected: {expected[i].Message} \r\n" +
						$" Actual:   {actual[i]}");
				}
			}

			//qq we do some naive calculations here that are inefficient but 'obviously correct'
			// we might want to consider breaking them out and writing some simple tests for them
			// just to be sure though.

			// after loading in the log we expect to be able to
			// 1. See a list of the collisions
			// 2. Find the metadata for each stream, by stream name.
			// 3. iterate through the payloads, with a name handle for the collisions
			//    and a hashhandle for the non-collisions.

			// 1. see a list of the stream collisions
			// 1a. naively calculate list of collisions
			var hashesInUse = new Dictionary<ulong, string>();
			var collidingStreams = new HashSet<string>();

			void RegisterUse(string streamId) {
				var hash = hasher.Hash(streamId);
				if (hashesInUse.TryGetValue(hash, out var user)) {
					if (user == streamId) {
						// in use by us. not a collision.
					} else {
						// collision. register both as collisions.
						collidingStreams.Add(streamId);
						collidingStreams.Add(user);
					}
				} else {
					// hash was not in use. so it isn't a collision.
					hashesInUse[hash] = streamId;
				}
			}

			foreach (var chunk in dbResult.Recs) {
				foreach (var record in chunk) {
					if (!(record is PrepareLogRecord prepare))
						continue;

					RegisterUse(prepare.EventStreamId);

					if (metastreamLookup.IsMetaStream(prepare.EventStreamId)) {
						RegisterUse(metastreamLookup.OriginalStreamOf(prepare.EventStreamId));
					}
				}
			}

			// 1b. assert list of collisions.
			Assert.Equal(collidingStreams.OrderBy(x => x), scavengeState.AllCollisions().OrderBy(x => x));

			//qq some other checks that look inside the scavenge state here because
			// - they are just being troublesome to maintain rather than helping to find problems
			// - the tests can still check the state and the trace if they want
			// - the output checks are catching most problems

			// 4. The records we expected to keep are kept
			// 5. The index entries we expected to be kept are kept
			if (keptRecords != null) {
				CheckRecords(keptRecords, dbResult);
//qqqqqqqqqqqqqqqqqqqqqqqq				CheckIndex(keptIndexEntries, indexScavenger.Scavenged);
			}

			return (scavengeState, dbResult);
		}







		//qq nicked from scavengetestscenario, will probably just use that class
		protected static void CheckRecords(LogRecord[][] expected, DbResult actual) {
			Assert.True(
				expected.Length == actual.Db.Manager.ChunksCount,
				"Wrong number of chunks. " +
				$"Expected {expected.Length}. Actual {actual.Db.Manager.ChunksCount}");

			for (int i = 0; i < expected.Length; ++i) {
				var chunk = actual.Db.Manager.GetChunk(i);

				var chunkRecords = new List<LogRecord>();
				var result = chunk.TryReadFirst();
				while (result.Success) {
					chunkRecords.Add(result.LogRecord);
					result = chunk.TryReadClosestForward((int)result.NextPosition);
				}

				Assert.True(
					expected[i].Length == chunkRecords.Count,
					$"Wrong number of records in chunk #{i}. " +
					$"Expected {expected[i].Length}. Actual {chunkRecords.Count}");

				for (int j = 0; j < expected[i].Length; ++j) {
					//qqqqqqq
					// for now null indicates there should be a record there but not what it should be
					// using to indicate the place of a scavengepoint
					//if (expected[i][j] == null)
					//	continue;

					Assert.True(
						expected[i][j] == chunkRecords[j],
						$"Wrong log record #{j} read from chunk #{i}. " +
						$"Expected {expected[i][j]}.\r\n" +
						$"Actual   {chunkRecords[j]}");
				}
			}
		}

		//qq this one reads the records out of actual.Recs, for use with the scaffolding implementations
		// until we transition.
		//protected static void CheckRecordsScaffolding(LogRecord[][] expected, DbResult actual) {
		//	Assert.True(
		//		expected.Length == actual.Db.Manager.ChunksCount,
		//		"Wrong number of chunks. " +
		//		$"Expected {expected.Length}. Actual {actual.Db.Manager.ChunksCount}");

		//	for (int i = 0; i < expected.Length; ++i) {
		//		var chunkRecords = actual.Recs[i].ToList();

		//		Assert.True(
		//			expected[i].Length == chunkRecords.Count,
		//			$"Wrong number of records in chunk #{i}. " +
		//			$"Expected {expected[i].Length}. Actual {chunkRecords.Count}");

		//		for (int j = 0; j < expected[i].Length; ++j) {
		//			// for now null indicates there should be a record there but not what it should be
		//			// using to indicate the place of a scavengepoint
		//			if (expected[i][j] == null)
		//				continue;

		//			Assert.True(
		//				expected[i][j].Equals(chunkRecords[j]),
		//				$"Wrong log record #{j} read from chunk #{i}.\r\n" +
		//				$"Expected {expected[i][j]}\r\n" +
		//				$"Actual   {chunkRecords[j]}");
		//		}
		//	}
		//}

		private static void CheckIndex(LogRecord[][] expected, LogRecord[][] actual) {
			if (expected == null) {
				// test didn't ask us to check the index
				return;
			}

			Assert.True(
				expected.Length == actual.Length,
				"IndexCheck. Wrong number of index-chunks. " +
				$"Expected {expected.Length}. Actual {actual.Length}");

			for (int i = 0; i < expected.Length; ++i) {
				var chunkRecords = actual[i];

				Assert.True(
					expected[i].Length == chunkRecords.Length,
					$"IndexCheck. Wrong number of records in index-chunk #{i}. " +
					$"Expected {expected[i].Length}. Actual {chunkRecords.Length}");

				for (int j = 0; j < expected[i].Length; ++j) {
					// for now null indicates there should be a record there but not what it should be
					// using to indicate the place of a scavengepoint
					if (expected[i][j] == null)
						continue;

					Assert.True(
						expected[i][j].Equals(chunkRecords[j]),
						$"IndexCheck. Wrong log record #{j} read from index-chunk #{i}.\r\n" +
						$"Expected {expected[i][j]}\r\n" +
						$"Actual   {chunkRecords[j]}");
				}
			}
		}

		class CompareOnlyMetadataAndTombstone : IEqualityComparer<OriginalStreamData> {
			public bool Equals(OriginalStreamData x, OriginalStreamData y) {
				if ((x == null) != (y == null))
					return false;

				if (x is null)
					return true;

				return
					x.IsTombstoned == y.IsTombstoned &&
					x.MaxCount == y.MaxCount &&
					x.MaxAge == y.MaxAge &&
					x.TruncateBefore == y.TruncateBefore;
			}

			public int GetHashCode(OriginalStreamData obj) {
				throw new NotImplementedException();
			}
		}
	}
}
