using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	// This datastructure is read and written to by the Accumulator/Calculator/Executors.
	// They contain the scavenge logic, this is just the holder of the data.
	//
	// we store data for metadata streams and for original streams, but we need to store
	// different data for each so we have two maps. we have one collision detector since
	// we need to detect collisions between all of the streams.
	// we don't need to store data for every original stream, only ones that need scavenging.
	public class ScavengeState<TStreamId> : IScavengeState<TStreamId> {

		private readonly CollisionDetector<TStreamId> _collisionDetector;

		// data stored keyed against metadata streams
		private readonly CollisionManager<TStreamId, MetastreamData> _metadatas;

		// data stored keyed against original (non-metadata) streams
		private readonly CollisionManager<TStreamId, EnrichedDiscardPoint> _originalStreamDatas;

		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;

		private readonly IScavengeMap<int, float> _chunkWeights;

		public ScavengeState(
			ILongHasher<TStreamId> hasher,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IScavengeMap<TStreamId, Unit> collisionStorage,
			IScavengeMap<ulong, MetastreamData> metaStorage,
			IScavengeMap<TStreamId, MetastreamData> metaCollisionStorage,
			IScavengeMap<ulong, EnrichedDiscardPoint> originalStorage,
			IScavengeMap<TStreamId, EnrichedDiscardPoint> originalCollisionStorage,
			IScavengeMap<int, float> chunkWeights,
			IScavengeMap<ulong, TStreamId> hashes) {


			//qq inject this so that in log v3 we can have a trivial implementation
			//qq to save us having to look up the stream names repeatedly
			_collisionDetector = new CollisionDetector<TStreamId>(
				//qq configurable cacheMaxCount
				new LruCachingScavengeMap<ulong, TStreamId>(hashes, cacheMaxCount: 10_000),
				collisionStorage,
				hasher);

			_hasher = hasher;
			_metastreamLookup = metastreamLookup;

			_metadatas = new CollisionManager<TStreamId, MetastreamData>(
				_hasher,
				_collisionDetector.IsCollision,
				metaStorage,
				metaCollisionStorage);

			_originalStreamDatas = new CollisionManager<TStreamId, EnrichedDiscardPoint>(
				_hasher,
				_collisionDetector.IsCollision,
				originalStorage,
				originalCollisionStorage);

			_chunkWeights = chunkWeights;
		}








		//
		// STUFF THAT CAME FROM COLLISION MANAGER
		//

		//qq method? property? enumerable? array? clunky allocations at the moment.
		public IEnumerable<TStreamId> Collisions() {
			return _collisionDetector.GetAllCollisions();
		}









		//
		// FOR ACCUMULATOR
		//

		public void DetectCollisions(TStreamId streamId) {
			var collisionResult = _collisionDetector.DetectCollisions(
				streamId,
				out var collision);

			if (collisionResult == CollisionResult.NewCollision) {
				_metadatas.NotifyCollision(collision);
				_originalStreamDatas.NotifyCollision(collision);
			}
		}

		public bool TryGetMetastreamData(TStreamId streamId, out MetastreamData streamData) =>
			_metadatas.TryGetValue(streamId, out streamData);
	
		public void SetMetastreamData(TStreamId streamId, MetastreamData streamData) {
			_metadatas[streamId] = streamData;
		}

		public void SetOriginalStreamData(TStreamId streamId, EnrichedDiscardPoint streamData) {
			_originalStreamDatas[streamId] = streamData;
		}





		//
		// FOR CALCULATOR
		//

		// the calculator needs to get the accumulated data for each scavengeable stream
		// it does not have and does not need to know the non colliding stream names.
		//qq consider making this a method?
		public IEnumerable<(StreamHandle<TStreamId>, MetastreamData)> MetastreamDatas =>
			_metadatas.Enumerate();

		public void SetOriginalStreamData(
			StreamHandle<TStreamId> handle,
			EnrichedDiscardPoint discardPoint) {

			_originalStreamDatas[handle] = discardPoint;
		}

		public bool TryGetOriginalStreamData(
			StreamHandle<TStreamId> handle,
			out EnrichedDiscardPoint discardPoint) =>

			_originalStreamDatas.TryGetValue(handle, out discardPoint);

		public bool TryGetChunkWeight(int chunkNumber, out float weight) =>
			_chunkWeights.TryGetValue(chunkNumber, out weight);

		public void SetChunkWeight(int chunkNumber, float weight) {
			_chunkWeights[chunkNumber] = weight;
		}





		//
		// FOR CHUNK EXECUTOR
		//

		public bool TryGetDiscardPoint(TStreamId streamId, out DiscardPoint discardPoint) {
			if (_metastreamLookup.IsMetaStream(streamId)) {
				if (!_metadatas.TryGetValue(streamId, out var metastreamData)) {
					discardPoint = default;
					return false;
				}

				discardPoint = metastreamData.DiscardPoint;

				return true;
			} else {
				if (!_originalStreamDatas.TryGetValue(streamId, out var originalStreamData)) {
					discardPoint = default;
					return false;
				}

				discardPoint = originalStreamData.DiscardPoint;
				return true;
			}
		}


		//
		// FOR INDEX EXECUTOR
		//

		public bool TryGetDiscardPoint(
			StreamHandle<TStreamId> handle,
			out DiscardPoint discardPoint) {

			// here we know that the handle is of the correct kind
			// but we do not know whether it is for a metastream or an originalstream.
			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					// not a collision, but we do not know whether it is a metastream or not.
					// check both maps (better if we didnt have to though..)
					return TryGetDiscardPointForOriginalStream(handle, out discardPoint)
						|| TryGetDiscardPointForMetadataStream(handle, out discardPoint);
				case StreamHandle.Kind.Id:
					// collision, but at least we can tell whether it is a metastream or not.
					// so just check one map.
					return _metastreamLookup.IsMetaStream(handle.StreamId)
						? TryGetDiscardPointForMetadataStream(handle, out discardPoint)
						: TryGetDiscardPointForOriginalStream(handle, out discardPoint);
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}

		private bool TryGetDiscardPointForMetadataStream(
			StreamHandle<TStreamId> handle,
			out DiscardPoint discardPoint) {

			if (!_metadatas.TryGetValue(handle, out var metastreamData)) {
				discardPoint = default;
				return false;
			}

			discardPoint = metastreamData.DiscardPoint;
			return true;
		}

		private bool TryGetDiscardPointForOriginalStream(
			StreamHandle<TStreamId> handle,
			out DiscardPoint discardPoint) {

			if (!_originalStreamDatas.TryGetValue(handle, out var streamData)) {
				discardPoint = default;
				return false;
			}

			discardPoint = streamData.DiscardPoint;
			return true;
		}

		public bool IsCollision(ulong streamHash) {
			//qq track these as we go rather than calculating each time on demand.
			var collidingHashes = new HashSet<ulong>();
			
			foreach (var collidingKey in _collisionDetector.GetAllCollisions()) {
				collidingHashes.Add(_hasher.Hash(collidingKey));
			}

			return collidingHashes.Contains(streamHash);
		}
	}
}
