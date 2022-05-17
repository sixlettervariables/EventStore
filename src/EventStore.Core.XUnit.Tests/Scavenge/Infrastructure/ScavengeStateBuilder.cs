using System;
using System.IO;
using System.Threading;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ScavengeStateBuilder : IDisposable {
		private readonly ILongHasher<string> _hasher;
		private readonly IMetastreamLookup<string> _metastreamLookup;

		private ScavengeState<string> _preexisting;
		private Tracer _tracer;
		private Type _cancelWhenCheckpointingType;
		private CancellationTokenSource _cancellationTokenSource;
		private Action<ScavengeState<string>> _mutateState;
		private SqliteScavengeBackend<string> _sqliteBackend;

		public ScavengeStateBuilder(
			ILongHasher<string> hasher,
			IMetastreamLookup<string> metastreamLookup) {

			_hasher = hasher;
			_metastreamLookup = metastreamLookup;
			_mutateState = x => { };
		}

		public ScavengeStateBuilder ExistingState(ScavengeState<string> state) {
			_preexisting = state;
			return this;
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

		public ScavengeState<string> Build() {
			var state = BuildInternal();
			_mutateState(state);
			return state;
		}

		private ScavengeState<string> BuildInternal(bool inMemory=true) {
			if (_preexisting != null)
				return _preexisting;

			if (!inMemory) {
				_sqliteBackend = new SqliteScavengeBackend<string>();
				_sqliteBackend.Initialize(Path.Combine("scavenge-state",Guid.NewGuid().ToString()));
				
				return new ScavengeState<string>(
					_hasher,
					_metastreamLookup,
					_sqliteBackend.CollisionStorage,
					_sqliteBackend.Hashes,
					_sqliteBackend.MetaStorage,
					_sqliteBackend.MetaCollisionStorage,
					_sqliteBackend.OriginalStorage,
					_sqliteBackend.OriginalCollisionStorage,
					_sqliteBackend.CheckpointStorage,
					_sqliteBackend.ChunkTimeStampRanges,
					_sqliteBackend.ChunkWeights,
					new TransactionManager<SqliteTransaction>(_sqliteBackend, _sqliteBackend.CheckpointStorage));
			}
			
			
			var collisionStorage = new InMemoryScavengeMap<string, Unit>();
			var hashesStorage = new InMemoryScavengeMap<ulong, string>();
			var metaStorage = new InMemoryMetastreamScavengeMap<ulong>();
			var metaCollisionStorage = new InMemoryMetastreamScavengeMap<string>();
			IOriginalStreamScavengeMap<ulong> originalStorage =
				new InMemoryOriginalStreamScavengeMap<ulong>();
			IOriginalStreamScavengeMap<string> originalCollisionStorage =
				new InMemoryOriginalStreamScavengeMap<string>();
			var checkpointStorage = new InMemoryScavengeMap<Unit, ScavengeCheckpoint>();
			var chunkTimeStampRangesStorage = new InMemoryScavengeMap<int, ChunkTimeStampRange>();
			var chunkWeightStorage = new InMemoryChunkWeightScavengeMap();
			ITransactionFactory<int> transactionFactory = new InMemoryTransactionFactory();

			if (_tracer != null)
				transactionFactory = new TracingTransactionFactory<int>(transactionFactory, _tracer);

			ITransactionManager transactionManager = new TransactionManager<int>(
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

		//qq properly dispose in tests
		public void Dispose() {
			_sqliteBackend?.Dispose();
		}
	}
}
