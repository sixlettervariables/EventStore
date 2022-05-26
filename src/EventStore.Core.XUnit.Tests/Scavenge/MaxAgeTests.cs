﻿using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.XUnit.Tests.Scavenge.Sqlite;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// for testing the maxage functionality specifically
	public class MaxAgeTests : SqliteDbPerTest<MaxAgeTests> {
		[Fact]
		public async Task simple_maxage() {
			// records kept in the index because they are 'maybe' expired
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Active),
						Rec.Write(t++, "ab-1", timestamp: Active),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnection(Fixture.DbConnection))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3, 4),
						x.Recs[1],
					},
					x => new[] {
						x.Recs[0],
						x.Recs[1],
					});
		}

		[Fact]
		public async Task keep_last_event() {
			// records kept in the index because they are 'maybe' expired
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnection(Fixture.DbConnection))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3),
						x.Recs[1],
					},
					x => new[] {
						x.Recs[0],
						x.Recs[1],
					});
		}

		[Fact]
		public async Task whole_chunk_expired() {
			// the records can be removed from the chunks and the index
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired))
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Active),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnection(Fixture.DbConnection))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(),
						x.Recs[1].KeepIndexes(0, 1),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task whole_chunk_expired_keep_last_event() {
			// the records can be removed from the chunks and the index
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired),
						Rec.Write(t++, "ab-1", timestamp: Expired))
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnection(Fixture.DbConnection))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2),
						x.Recs[1].KeepIndexes(0),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task whole_chunk_active() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Active),
						Rec.Write(t++, "ab-1", timestamp: Active),
						Rec.Write(t++, "ab-1", timestamp: Active))
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnection(Fixture.DbConnection))
				.RunAsync(
					x => new[] {
						x.Recs[0],
						x.Recs[1],
						x.Recs[2],
					});
		}
	}
}
