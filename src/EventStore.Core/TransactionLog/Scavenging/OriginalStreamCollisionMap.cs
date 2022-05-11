using System;
using EventStore.Core.Data;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class OriginalStreamCollisionMap<TStreamId> :
		CollisionMap<TStreamId, OriginalStreamData> {

		private readonly ILongHasher<TStreamId> _hasher;
		private readonly Func<TStreamId, bool> _isCollision;
		private readonly IOriginalStreamScavengeMap<ulong> _nonCollisions;
		private readonly IOriginalStreamScavengeMap<TStreamId> _collisions;

		public OriginalStreamCollisionMap(
			ILongHasher<TStreamId> hasher,
			Func<TStreamId, bool> isCollision,
			IOriginalStreamScavengeMap<ulong> nonCollisions,
			IOriginalStreamScavengeMap<TStreamId> collisions) :
			base(
				hasher, isCollision, nonCollisions, collisions) {

			_hasher = hasher;
			_isCollision = isCollision;
			_nonCollisions = nonCollisions;
			_collisions = collisions;
		}

		public void SetTombstone(TStreamId streamId) {
			if (_isCollision(streamId))
				_collisions.SetTombstone(streamId);
			else
				_nonCollisions.SetTombstone(_hasher.Hash(streamId));
		}

		public void SetMetadata(TStreamId streamId, StreamMetadata metadata) {
			if (_isCollision(streamId))
				_collisions.SetMetadata(streamId, metadata);
			else
				_nonCollisions.SetMetadata(_hasher.Hash(streamId), metadata);
		}

		public void SetDiscardPoints(
			StreamHandle<TStreamId> handle,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint) {

			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					_nonCollisions.SetDiscardPoints(
						handle.StreamHash,
						discardPoint,
						maybeDiscardPoint);
					break;
				case StreamHandle.Kind.Id:
					_collisions.SetDiscardPoints(
						handle.StreamId,
						discardPoint,
						maybeDiscardPoint);
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}

		public bool TryGetChunkExecutionInfo(TStreamId streamId, out ChunkExecutionInfo info) =>
			_isCollision(streamId)
				? _collisions.TryGetChunkExecutionInfo(streamId, out info)
				: _nonCollisions.TryGetChunkExecutionInfo(_hasher.Hash(streamId), out info);

		public void DeleteTombstoned() {
			_collisions.DeleteTombstoned();
			_nonCollisions.DeleteTombstoned();
		}
	}
}
