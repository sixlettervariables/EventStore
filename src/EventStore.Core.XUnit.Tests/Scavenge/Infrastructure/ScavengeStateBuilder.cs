using System;
using System.Threading;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ScavengeStateBuilder {
		private readonly ILongHasher<string> _hasher;
		private readonly IMetastreamLookup<string> _metastreamLookup;

		private Tracer _tracer;
		private SqliteConnection _connection;
		private Type _cancelWhenCheckpointingType;
		private CancellationTokenSource _cancellationTokenSource;
		private Action<ScavengeState<string>> _mutateState;

		public ScavengeStateBuilder(
			ILongHasher<string> hasher,
			IMetastreamLookup<string> metastreamLookup) {

			_hasher = hasher;
			_metastreamLookup = metastreamLookup;
			_mutateState = x => { };
		}

		public ScavengeStateBuilder TransformBuilder(Func<ScavengeStateBuilder, ScavengeStateBuilder> f) =>
			f(this);

		public ScavengeStateBuilder CancelWhenCheckpointing(Type type, CancellationTokenSource cts) {
			_cancelWhenCheckpointingType = type;
			_cancellationTokenSource = cts;
			return this;
		}

		public ScavengeStateBuilder MutateState(Action<ScavengeState<string>> f) {
			var wrapped = _mutateState;
			_mutateState = state => {
				wrapped(state);
				f(state);
			};
			return this;
		}

		public ScavengeStateBuilder WithTracer(Tracer tracer) {
			_tracer = tracer;
			return this;
		}

		public ScavengeStateBuilder WithConnection(SqliteConnection connection) {
			_connection = connection;
			return this;
		}

		public ScavengeState<string> Build() {
			var state = BuildInternal();
			_mutateState(state);
			return state;
		}

		private ScavengeState<string> BuildInternal() {
			if (_connection == null)
				throw new Exception("call WithConnection");

			var sqlite = new SqliteScavengeBackend<string>();
			sqlite.Initialize(_connection);

			var collisionStorage = sqlite.CollisionStorage;
			var hashesStorage = sqlite.Hashes;
			var metaStorage = sqlite.MetaStorage;
			var metaCollisionStorage = sqlite.MetaCollisionStorage;
			var originalStorage = sqlite.OriginalStorage;
			var originalCollisionStorage = sqlite.OriginalCollisionStorage;
			var checkpointStorage = sqlite.CheckpointStorage;
			var chunkTimeStampRangesStorage = sqlite.ChunkTimeStampRanges;
			var chunkWeightStorage = sqlite.ChunkWeights;
			var transactionFactory = sqlite.TransactionFactory;

			if (_tracer != null)
				transactionFactory = new TracingTransactionFactory<SqliteTransaction>(transactionFactory, _tracer);

			ITransactionManager transactionManager = new TransactionManager<SqliteTransaction>(
				transactionFactory,
				checkpointStorage);

			transactionManager = new AdHocTransactionManager(
				transactionManager,
				(continuation, checkpoint) => {
					if (checkpoint.GetType() == _cancelWhenCheckpointingType) {
						_cancellationTokenSource.Cancel();
					}
					continuation(checkpoint);
				});

			if (_tracer != null) {
				transactionManager = new TracingTransactionManager(transactionManager, _tracer);
				originalStorage = new TracingOriginalStreamScavengeMap<ulong>(originalStorage, _tracer);
				originalCollisionStorage =
					new TracingOriginalStreamScavengeMap<string>(originalCollisionStorage, _tracer);
			}

			var scavengeState = new ScavengeState<string>(
				_hasher,
				_metastreamLookup,
				collisionStorage,
				hashesStorage,
				metaStorage,
				metaCollisionStorage,
				originalStorage,
				originalCollisionStorage,
				checkpointStorage,
				chunkTimeStampRangesStorage,
				chunkWeightStorage,
				transactionManager);

			return scavengeState;
		}
	}
}
