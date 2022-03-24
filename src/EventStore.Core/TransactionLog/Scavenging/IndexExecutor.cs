using System;
using EventStore.Core.Index;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class IndexExecutor<TStreamId> : IIndexExecutor<TStreamId> {
		private readonly IIndexScavenger _indexScavenger;
		private readonly IChunkReaderForIndexExecutor<TStreamId> _streamLookup;

		public IndexExecutor(
			IIndexScavenger indexScavenger,
			IChunkReaderForIndexExecutor<TStreamId> streamLookup) {

			_indexScavenger = indexScavenger;
			_streamLookup = streamLookup;
		}

		public void Execute(
			IScavengeStateForIndexExecutor<TStreamId> state,
			IIndexScavengerLog scavengerLogger) {

			_indexScavenger.ScavengeIndex(
				shouldKeep: GenShouldKeep(state),
				log: scavengerLogger,
				//qq pass through a cancellation token
				cancellationToken: default);
		}

		private Func<IndexEntry, bool> GenShouldKeep(IScavengeStateForIndexExecutor<TStreamId> state) {
			//qq probably need some tests to execise the cached variables a bit
			// cache some info between invocations of ShouldKeep since it will typically be invoked
			// repeatedly with the same stream hash.
			//
			// invariants, guaranteed at the beginning and end of each Invokation of ShouldKeep:
			//  (a) currentHash is not null =>
			//         currentHashIsCollision iff currentHash is a collision
			//
			//  (b) currentHash is not null && !currentHashIsCollision =>
			//         currentDiscardPoint is the discardpoint of the unique stream
			//         that hashes to currentHash.
			//
			var currentHash = (ulong?)null;
			var currentHashIsCollision = false;
			var currentDiscardPoint = DiscardPoint.KeepAll;

			bool ShouldKeep(IndexEntry indexEntry) {
				//qq throttle?

				if (currentHash != indexEntry.Stream || currentHashIsCollision) {
					// currentHash != indexEntry.Stream || currentHashIsCollision
					// we are on to a new stream, or the hash collides so we _might_ be on to a new stream.

					// bring currentHash up to date.
					currentHash = indexEntry.Stream;
					// re-establish (a)
					currentHashIsCollision = state.IsCollision(indexEntry.Stream);

					StreamHandle<TStreamId> handle = default;

					if (currentHashIsCollision) {
						// (b) is re-established because currentHashIsCollision is true
						// collision, so the hash itself does not identify the stream. need to look it up.
						if (!_streamLookup.TryGetStreamId(indexEntry.Position, out var streamId)) {
							// there is no record at this position to get the stream from.
							// we should definitely discard the entry (just like old index scavenge does)
							// we can't even tell which stream it is for.
							return false;
						} else {
							// we got a streamId, which means we must have found a record at this
							// position, but that doesn't necessarily mean we want to keep the IndexEntry
							// the log record might still exist only because its chunk hasn't reached
							// the threshold.
							handle = StreamHandle.ForStreamId(streamId);
						}
					} else {
						// not a collision, we can get the discard point by hash.
						handle = StreamHandle.ForHash<TStreamId>(currentHash.Value);
					}

					//qq memoize to speed up other ptables?
					// ^ (consider this generally for the scavenge state)
					// re-establish (b)
					if (!state.TryGetDiscardPoint(handle, out currentDiscardPoint)) {
						// this stream has no discard point. keep everything.
						currentDiscardPoint = DiscardPoint.KeepAll;
						return true;
					}
				} else {
					// same hash as the previous invocation, and it is not a collision, so it must be for
					// the same stream, so the currentDiscardPoint applies.
					// invariants already established.
				}

				return !currentDiscardPoint.ShouldDiscard(indexEntry.Version);
			}

			return ShouldKeep;
		}

	}
}
