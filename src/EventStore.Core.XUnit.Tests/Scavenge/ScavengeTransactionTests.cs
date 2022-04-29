using System;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ScavengeTransactionTests {
		class MockTransactionBackend : ITransactionBackend {
			public int BeginCount { get; private set; }
			public int CommitCount { get; private set; }
			public int RollbackCount { get; private set; }

			public void Begin() {
				BeginCount++;
			}

			public void Commit() {
				CommitCount++;
			}

			public void Rollback() {
				RollbackCount++;
			}
		}

		[Fact]
		public void CanCommitThenBegin() {
			var storage = new InMemoryScavengeMap<Unit, ScavengeCheckpoint>();
			var backend = new MockTransactionBackend();
			var sut = new ScavengeTransaction(backend, storage);

			var expectedCheckpoint = new ScavengeCheckpoint.Accumulating(
				new ScavengePoint(default, default, default),
				5);

			Assert.Equal(0, backend.BeginCount);
			Assert.Equal(0, backend.CommitCount);
			Assert.Equal(0, backend.RollbackCount);

			sut.Begin();

			Assert.Equal(1, backend.BeginCount);
			Assert.Equal(0, backend.CommitCount);
			Assert.Equal(0, backend.RollbackCount);

			sut.Commit(expectedCheckpoint);
			Assert.True(storage.TryGetValue(Unit.Instance, out var actualCheckpoint));
			Assert.Equal(expectedCheckpoint, actualCheckpoint);

			Assert.Equal(1, backend.BeginCount);
			Assert.Equal(1, backend.CommitCount);
			Assert.Equal(0, backend.RollbackCount);

			sut.Begin();

			Assert.Equal(2, backend.BeginCount);
			Assert.Equal(1, backend.CommitCount);
			Assert.Equal(0, backend.RollbackCount);
		}

		[Fact]
		public void CanRollbackThenBegin() {
			var backend = new MockTransactionBackend();
			var sut = new ScavengeTransaction(
				backend,
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			Assert.Equal(0, backend.BeginCount);
			Assert.Equal(0, backend.CommitCount);
			Assert.Equal(0, backend.RollbackCount);

			sut.Begin();

			Assert.Equal(1, backend.BeginCount);
			Assert.Equal(0, backend.CommitCount);
			Assert.Equal(0, backend.RollbackCount);

			sut.Rollback();

			Assert.Equal(1, backend.BeginCount);
			Assert.Equal(0, backend.CommitCount);
			Assert.Equal(1, backend.RollbackCount);

			sut.Begin();

			Assert.Equal(2, backend.BeginCount);
			Assert.Equal(0, backend.CommitCount);
			Assert.Equal(1, backend.RollbackCount);
		}

		[Fact]
		public void CannotBeginTwice() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			sut.Begin();

			Assert.Throws<InvalidOperationException>(() => {
				sut.Begin();
			});
		}

		[Fact]
		public void CannotCommitTwice() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			sut.Begin();
			sut.Commit(null);

			Assert.Throws<InvalidOperationException>(() => {
				sut.Commit(null);
			});
		}

		[Fact]
		public void CannotCommitThenRollback() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			sut.Begin();
			sut.Commit(null);

			Assert.Throws<InvalidOperationException>(() => {
				sut.Rollback();
			});
		}

		[Fact]
		public void CannotRollbackTwice() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			sut.Begin();
			sut.Rollback();

			Assert.Throws<InvalidOperationException>(() => {
				sut.Rollback();
			});
		}

		[Fact]
		public void CannotRollbackThenCommit() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			sut.Begin();
			sut.Rollback();

			Assert.Throws<InvalidOperationException>(() => {
				sut.Commit(null);
			});
		}

		[Fact]
		public void CannotCommitWithoutBeginning() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			Assert.Throws<InvalidOperationException>(() => {
				sut.Commit(null);
			});
		}

		[Fact]
		public void CannotRollbackWithoutBeginning() {
			var sut = new ScavengeTransaction(
				new MockTransactionBackend(),
				new InMemoryScavengeMap<Unit, ScavengeCheckpoint>());

			Assert.Throws<InvalidOperationException>(() => {
				sut.Rollback();
			});
		}
	}
}
