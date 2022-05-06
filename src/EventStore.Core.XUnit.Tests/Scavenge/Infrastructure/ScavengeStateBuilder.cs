using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ScavengeStateBuilder {
		private readonly ILongHasher<string> _hasher;
		private readonly IMetastreamLookup<string> _metastreamLookup;

		private ScavengeState<string> _preexisting;
		private Tracer _tracer;
		private Action<ScavengeState<string>> _mutateState;

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

		private ScavengeState<string> BuildInternal() {
			if (_preexisting != null)
				return _preexisting;

			var collisionStorage = new InMemoryScavengeMap<string, Unit>();
			var hashesStorage = new InMemoryScavengeMap<ulong, string>();
			var metaStorage = new InMemoryScavengeMap<ulong, DiscardPoint>();
			var metaCollisionStorage = new InMemoryScavengeMap<string, DiscardPoint>();
			IOriginalStreamScavengeMap<ulong> originalStorage =
				new InMemoryOriginalStreamScavengeMap<ulong>();
			IOriginalStreamScavengeMap<string> originalCollisionStorage =
				new InMemoryOriginalStreamScavengeMap<string>();
			var checkpointStorage = new InMemoryScavengeMap<Unit, ScavengeCheckpoint>();
			var chunkTimeStampRangesStorage = new InMemoryScavengeMap<int, ChunkTimeStampRange>();
			var chunkWeightStorage = new InMemoryChunkWeightScavengeMap();
			ITransactionBackend transactionBackend = new InMemoryTransactionBackend();

			if (_tracer != null)
				transactionBackend = new TracingTransactionBackend(transactionBackend, _tracer);

			ITransaction transaction = new ScavengeTransaction(
				transactionBackend,
				checkpointStorage);

			if (_tracer != null) {
				transaction = new TracingTransaction(transaction, _tracer);
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
				transaction);

			return scavengeState;
		}
	}
}
