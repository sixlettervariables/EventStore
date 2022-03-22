using System;
using EventStore.Core.Index;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq tbc if we need our own implementation of this or can use the regular one.
	// also tbc where it should be constructed
	public class MyScavengerLog : IIndexScavengerLog {
		public void IndexTableNotScavenged(
			int level,
			int index,
			TimeSpan elapsed,
			long entriesKept,
			string errorMessage) {

			throw new NotImplementedException();
		}

		public void IndexTableScavenged(
			int level,
			int index,
			TimeSpan elapsed,
			long entriesDeleted,
			long entriesKept,
			long spaceSaved) {

			throw new NotImplementedException();
		}
	}

	public class IndexExecutor<TStreamId> : IIndexExecutor<TStreamId> {
		private readonly IIndexScavenger _indexScavenger;
		private readonly IChunkReaderForIndexExecutor<TStreamId> _streamLookup;

		public IndexExecutor(
			IIndexScavenger indexScavenger,
			IChunkReaderForIndexExecutor<TStreamId> streamLookup) {

			_indexScavenger = indexScavenger;
			_streamLookup = streamLookup;
		}

		public void Execute(IScavengeStateForIndexExecutor<TStreamId> state) {
			_indexScavenger.ScavengeIndex(
				shouldKeep: GenShouldKeep(state),
				log: new MyScavengerLog(),
				//qq pass through a cancellation token
				cancellationToken: default);

		}

		private Func<IndexEntry, bool> GenShouldKeep(IScavengeStateForIndexExecutor<TStreamId> state) {
			//qq check/test
			var currentHash = (ulong?)null;
			var currentHashIsCollision = false;
			var discardPoint = DiscardPoint.KeepAll;

			bool ShouldKeep(IndexEntry indexEntry) {
				//qq to decide whether to keep an index entry we need to 
				// 1. determine which stream it is for
				// 2. look up the discard point for that stream
				// 3. see if it is to be discarded.
				if (currentHash != indexEntry.Stream || currentHashIsCollision) {
					// on to a new stream, get its discard point.
					currentHash = indexEntry.Stream;
					currentHashIsCollision = state.IsCollision(indexEntry.Stream);

					discardPoint = GetDiscardPoint(
						state,
						indexEntry.Position,
						currentHash.Value,
						currentHashIsCollision);
				}

				return !discardPoint.ShouldDiscard(indexEntry.Version);
			}

			return ShouldKeep;
		}

		private DiscardPoint GetDiscardPoint(
			IScavengeStateForIndexExecutor<TStreamId> state,
			long position,
			ulong hash,
			bool isCollision) {

			StreamHandle<TStreamId> handle = default;

			if (isCollision) {
				// collision, we need to get the event for this position, see what stream it is
				// really for, and look up the discard point by streamId.

				if (_streamLookup.TryGetStreamId(position, out var streamId)) {
					handle = StreamHandle.ForStreamId(streamId);
				} else {
					//qq how unexpected is this, throw, or make this a TryGetDiscardPoint
					// presumably this can happen if the record was scavenged
					throw new Exception("lksjfw");
				}

			} else {
				// not a collision, we can get the discard point by hash.
				handle = StreamHandle.ForHash<TStreamId>(hash);
			}

			return state.TryGetDiscardPoint(handle, out var discardPoint)
				? discardPoint
				: DiscardPoint.KeepAll;
		}
	}
}
