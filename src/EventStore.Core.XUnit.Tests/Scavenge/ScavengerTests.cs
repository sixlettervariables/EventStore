using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.LogV2;
using EventStore.Core.Services;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ScavengerTests {
		private static readonly StreamMetadata _meta1 = new StreamMetadata(maxCount: 1);
		private static readonly StreamMetadata _meta2 = new StreamMetadata(maxCount: 2);

		//qq there is Rec.TransSt and TransEnd.. what do prepares and commits mean here without those?
		// probably applies to every test in here
		[Fact]
		public void Trivial() {
			CreateScenario(x => x
				.Chunk(
					// the first letter of the stream name determines its hash value
					// a-1:       a stream called "a-1" which hashes to "a"
					Rec.Prepare(0, "ab-1"),

					// setting metadata for a-1, which does not collide with a-1
					//qq um this isn't in a metadata stream so it probably wont be recognised as metadata
					// instead the stream should be called "ma1" which hashes to #a "$$ma1" which hashes
					// to #m and the hasher chooses which character depending on whether it is a metadta
					// stream.
					Rec.Prepare(1, "$$ab-1", "$metadata", metadata: _meta1))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void seen_stream_before() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "ab-1"))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void collision() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "ab-2"))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadata_non_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "$$ab-1", "$metadata", metadata: _meta1))
				.CompleteLastChunk())
				.Run();
		}

		//qq now that we are keying on the metadta streams, does that mean that we don't
		// need to many cases here? like whether or not the original streams collide might not be
		// relevant any more.
		//
		//qqqqqqqqqqqqq do we want to bake tombstones into here as well
		[Fact]
		public void metadata_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "aa-1"),
					Rec.Prepare(1, "$$aa-1", "$metadata", metadata: _meta1))
				.CompleteLastChunk())
				.Run();
		}

		//qq this would fail if we checked that looking up the metadatas per stream gives us
		// the right metadatas. ca-2 has no metadata but we would find _meta1 anyway.
		[Fact]
		public void darn() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "cb-2"))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadatas_for_different_streams_non_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "$$cd-2", "$metadata", metadata: _meta2))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadatas_for_different_streams_all_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$aa-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "$$aa-2", "$metadata", metadata: _meta2))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadatas_for_different_streams_original_streams_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "$$cb-2", "$metadata", metadata: _meta2))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadatas_for_different_streams_meta_streams_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "$$ac-2", "$metadata", metadata: _meta2))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadatas_for_different_streams_original_and_meta_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "$$ab-2", "$metadata", metadata: _meta2))
				.CompleteLastChunk())
				.Run();
		}

		[Fact]
		public void metadatas_for_different_streams_cross_colliding() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(1, "$$ba-2", "$metadata", metadata: _meta2))
				.CompleteLastChunk())
				.Run();
		}

		//qq we are expecting to actually get something scavenged here
		// use this as a vehicle to
		//  a) get it scavenged
		//  b) test that it was scavenged successfully. <- compare to the normal scavenge tests
		//qqq we will probably want a set of these to test out collisions when there are tombstones
		// and a set for collisions when there is metadata (maybe combination metadata and tombstone)
		// but shouldn't need to test different kinds of metadata
		[Fact]
		public void simple_tombstone() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Delete(1, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(1)
				});
		}

		[Fact]
		public void single_tombstone() {
			CreateScenario(x => x
				.Chunk(
					Rec.Delete(1, "ab-1"))
				.CompleteLastChunk())
				.Run();
		}

		//qq refactor to base class?
		private Scenario CreateScenario(Func<TFChunkDbCreationHelper, TFChunkDbCreationHelper> createDb) {
			return new Scenario(createDb);
		}

		class Scenario {
			private readonly Func<TFChunkDbCreationHelper, TFChunkDbCreationHelper> _createDb;

			public Scenario(
				Func<TFChunkDbCreationHelper, TFChunkDbCreationHelper> createDb) {
				_createDb = createDb;
			}

			public void Run(Func<DbResult, LogRecord[][]> getExpectedKeptRecords = null) {
				RunScenario(
					x => _createDb(x).CreateDb(),
					getExpectedKeptRecords);
			}

			private static void RunScenario(
				Func<TFChunkDbCreationHelper, DbResult> createDb,
				Func<DbResult, LogRecord[][]> getExpectedKeptRecords) {

				//qq use directory fixture, or memdb. pattern in ScavengeTestScenario.cs
				var pathName = @"unused currently";
				var dbConfig = TFChunkHelper.CreateDbConfig(pathName, 0, chunkSize: 1024 * 1024, memDb: true);

				var dbCreator = new TFChunkDbCreationHelper(dbConfig);

				var dbResult = createDb(dbCreator);
				var keptRecords = getExpectedKeptRecords == null
					? null
					: getExpectedKeptRecords(dbResult);

				// the log. will mutate as we scavenge.
				var log = dbResult.Recs;

				// original log. will not mutate, for calculating expected results.
				var originalLog = log.ToArray();

				var hasher = new HumanReadableHasher();
				var metastreamLookup = new LogV2SystemStreams();

				var collisionStorage = new InMemoryScavengeMap<string, Unit>();
				var metaStorage = new InMemoryScavengeMap<ulong, MetastreamData>();
				var metaCollisionStorage = new InMemoryScavengeMap<string, MetastreamData>();
				var originalStorage = new InMemoryScavengeMap<ulong, EnrichedDiscardPoint>();
				var originalCollisionStorage = new InMemoryScavengeMap<string, EnrichedDiscardPoint>();
				var chunkWeightStorage = new InMemoryScavengeMap<int, float>();

				//qq date storage

				var scavengeState = new ScavengeState<string>(
					hasher,
					metastreamLookup,
					collisionStorage,
					metaStorage,
					metaCollisionStorage,
					originalStorage,
					originalCollisionStorage,
					chunkWeightStorage,
					new ScaffoldHashUsageChecker(hasher, log));

				//qq we presumably want to actually get this from the log.
				var scavengePoint = new ScavengePoint {
					EffectiveNow = new DateTime(2022, 1, 1, 15, 55, 00),
					Position = 123, //qq
				};

				var indexScavenger = new ScaffoldStuffForIndexExecutor(originalLog, hasher);
				var sut = new Scavenger<string>(
					scavengeState,
					new Accumulator<string>(
						hasher: hasher,
						metastreamLookup: metastreamLookup,
						chunkReader: new ScaffoldChunkReaderForAccumulator(log)),
					new Calculator<string>(
						index: new ScaffoldIndexForScavenge(log, hasher)),
					new ChunkExecutor<string, ScaffoldChunk>(
						chunkManager: new ScaffoldChunkManagerForScavenge(
							chunkSize: dbConfig.ChunkSize,
							log: log),
						chunkSize: dbConfig.ChunkSize),
					new IndexExecutor<string>(
						indexScavenger: indexScavenger,
						streamLookup: new ScaffoldChunkReaderForIndexExecutor(log)),
					new ScaffoldScavengePointSource(scavengePoint));

				sut.Start(); //qq irl how do we know when its done

				//qqqqqqqq ACCUMULATE (the accumulator does this, remove once the tests are passing)
				// iterate through the log, detecting collisions and accumulating metadatas
				//for (var i = 0; i < log.Length; i++) {
				//	var record = log[i];
				//	scavengeState.DetectCollisions(record.StreamName, i);

				//	if (metastreamLookup.IsMetaStream(record.StreamName)) {
				//		scavengeState.SetMetastreamData(record.StreamName, record.MetastreamData);
				//	}
				//}

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
				foreach (var chunk in originalLog) {
					foreach (var record in chunk) {
						if (!(record is PrepareLogRecord prepare))
							continue;

						var hash = hasher.Hash(prepare.EventStreamId);
						if (hashesInUse.TryGetValue(hash, out var user)) {
							if (user == prepare.EventStreamId) {
								// in use by us. not a collision.
							} else {
								// collision. register both as collisions.
								collidingStreams.Add(prepare.EventStreamId);
								collidingStreams.Add(user);
							}
						} else {
							// hash was not in use. so it isn't a collision.
							hashesInUse[hash] = prepare.EventStreamId;
						}
					}
				}

				// 1b. assert list of collisions.
				Assert.Equal(collidingStreams.OrderBy(x => x), scavengeState.Collisions().OrderBy(x => x));


				// 2. Find the metadata for each stream, by stream name
				// 2a. naively calculate the expected metadata per stream
				var expectedMetadataPerStream = new Dictionary<string, MetastreamData>();
				foreach (var chunk in originalLog) {
					foreach (var record in chunk) {
						if (!(record is PrepareLogRecord prepare))
							continue;

						if (prepare.EventType != SystemEventTypes.StreamMetadata)
							continue;

						//qq need?
						//if (!SystemStreams.IsMetastream(prepare.EventStreamId))
						//	continue;

						var metadata = StreamMetadata.FromJsonBytes(prepare.Data);

						var metaStreamData = new MetastreamData {
							MaxAge = metadata.MaxAge,
							MaxCount = metadata.MaxCount,
							TruncateBefore = metadata.TruncateBefore,
							//qq need? DiscardPoint = ?,
							//qq need? OriginalStreamHash = ?
						};

						expectedMetadataPerStream[prepare.EventStreamId] = metaStreamData;
					}
				}

				// 2b. assert that we can find each one
				var compareOnlyMetadata = new CompareOnlyMetadata();
				foreach (var kvp in expectedMetadataPerStream) {
					if (!scavengeState.TryGetMetastreamData(kvp.Key, out var meta))
						meta = MetastreamData.Empty;
					Assert.Equal(kvp.Value, meta, compareOnlyMetadata);
				}

				// 3. Iterate through the metadatas, find the appropriate handles.
				// 3a. naively calculate the expected handles. one for each metadata, some by hash,
				// some by streamname
				var expectedHandles = expectedMetadataPerStream
					.Select(kvp => {
						var stream = kvp.Key;
						var metadata = kvp.Value;

						return collidingStreams.Contains(stream)
							? (StreamHandle.ForStreamId(stream), metadata)
							: (StreamHandle.ForHash<string>(hasher.Hash(stream)), metadata);
					})
					.Select(x => (x.Item1.ToString(), x.metadata))
					.OrderBy(x => x.Item1);

				// 3b. compare to the actual handles.
				var actual = scavengeState
					.MetastreamDatas
					.Select(x => (x.Item1.ToString(), x.Item2))
					.OrderBy(x => x.Item1);

				// compare the handles
				Assert.Equal(expectedHandles.Select(x => x.Item1), actual.Select(x => x.Item1));

				// compare the metadatas
				Assert.Equal(
					expectedHandles.Select(x => x.metadata),
					actual.Select(x => x.Item2),
					compareOnlyMetadata);

				// 4. The records we expected to keep are kept
				// 5. The index entries we expected to be kept are kept
				if (keptRecords != null) {
					CheckRecordsScaffolding(keptRecords, dbResult);
					CheckIndex(keptRecords, indexScavenger.Scavenged);
				}

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
						Assert.True(
							expected[i][j] == chunkRecords[j],
							$"Wrong log record #{j} read from chunk #{i}. " +
							$"Expected {expected[i][j]}. Actual {chunkRecords[j]}");
					}
				}
			}

			//qq this one reads the records out of actual.Recs, for use with the scaffolding implementations
			// until we transition.
			protected static void CheckRecordsScaffolding(LogRecord[][] expected, DbResult actual) {
				Assert.True(
					expected.Length == actual.Db.Manager.ChunksCount,
					"Wrong number of chunks. " +
					$"Expected {expected.Length}. Actual {actual.Db.Manager.ChunksCount}");

				for (int i = 0; i < expected.Length; ++i) {
					var chunkRecords = actual.Recs[i].ToList();

					Assert.True(
						expected[i].Length == chunkRecords.Count,
						$"Wrong number of records in chunk #{i}. " +
						$"Expected {expected[i].Length}. Actual {chunkRecords.Count}");

					for (int j = 0; j < expected[i].Length; ++j) {
						Assert.True(
							expected[i][j].Equals(chunkRecords[j]),
							$"Wrong log record #{j} read from chunk #{i}.\r\n" +
							$"Expected {expected[i][j]}\r\n" +
							$"Actual   {chunkRecords[j]}");
					}
				}
			}

			private static void CheckIndex(LogRecord[][] expected, LogRecord[][] actual) {
				Assert.True(
					expected.Length == actual.Length,
					"IndexCheck. Wrong number of chunks. " +
					$"Expected {expected.Length}. Actual {actual.Length}");

				for (int i = 0; i < expected.Length; ++i) {
					var chunkRecords = actual[i];

					Assert.True(
						expected[i].Length == chunkRecords.Length,
						$"IndexCheck. Wrong number of records in chunk #{i}. " +
						$"Expected {expected[i].Length}. Actual {chunkRecords.Length}");

					for (int j = 0; j < expected[i].Length; ++j) {
						Assert.True(
							expected[i][j].Equals(chunkRecords[j]),
							$"IndexCheck. Wrong log record #{j} read from chunk #{i}.\r\n" +
							$"Expected {expected[i][j]}\r\n" +
							$"Actual   {chunkRecords[j]}");
					}
				}
			}

			class CompareOnlyMetadata : IEqualityComparer<MetastreamData> {
				public bool Equals(MetastreamData x, MetastreamData y) {
					if ((x == null) != (y == null))
						return false;

					if (x is null)
						return true;

					return
						x.MaxCount == y.MaxCount &&
						x.MaxAge == y.MaxAge &&
						x.TruncateBefore == y.TruncateBefore;
				}

				public int GetHashCode(MetastreamData obj) {
					throw new NotImplementedException();
				}
			}
		}
	}

	public static class ArrayExtensions {
		public static T[] KeepIndexes<T>(this T[] self, params int[] indexes) =>
			self.Where((x, i) => indexes.Contains(i)).ToArray();
	}

}
