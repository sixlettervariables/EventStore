﻿using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {
	// this class efficiently stores/retrieves data against keys that very rarely but sometimes have a
	// hash collision.
	// when there is a hash collision the key is stored explicitly with the value
	// otherwise it only stores the hashes and the values.
	//
	// in practice this allows us to
	//   1. store data for lots of streams with much reduced size and complexity
	//      because we rarely if ever need to store the stream name, and the hashes are fixed size
	//   2. when being read, inform the caller whether the key hash collides or not
	//
	// for retrieval, if you have the key then you can always get the value
	// if you have the hash then what? //qq
	// and you can iterate through everything.


	//qq rename
	public class CollisionManager<TKey, TValue> {
		private readonly IScavengeMap<ulong, TValue> _nonCollisions;
		private readonly IScavengeMap<TKey, TValue> _collisions;
		private readonly ILongHasher<TKey> _hasher;
		private readonly Func<TKey, bool> _isCollision;

		public CollisionManager(
			ILongHasher<TKey> hasher,
			Func<TKey, bool> isCollision,
			IScavengeMap<ulong, TValue> nonCollisions,
			IScavengeMap<TKey, TValue> collisions) {

			_hasher = hasher;
			_isCollision = isCollision;
			_nonCollisions = nonCollisions;
			_collisions = collisions;
		}

		public bool TryGetValue(TKey key, out TValue value) {
			//qq important precondition: the key must already be checked for whether it collides.
			return _isCollision(key)
				? _collisions.TryGetValue(key, out value)
				: _nonCollisions.TryGetValue(_hasher.Hash(key), out value);
		}

		//qq
		// perhaps the difference is when providing a handle we are saying we know how we want to look
		// this up (we know if it is a collision), and if we provide the full key we are saying 
		// 'dont care whether it is a collision or not'.
		public bool TryGetValue(StreamHandle<TKey> handle, out TValue value) {
			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					return _nonCollisions.TryGetValue(handle.StreamHash, out value);
				case StreamHandle.Kind.Id:
					return _collisions.TryGetValue(handle.StreamId, out value);
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}

		//qqqqqq consider this api and its preconditions. would it be better to have an indexer setter
		// and indeed an indexer getter
		// make sure to document the preconditions.
		public TValue this[StreamHandle<TKey> handle] {
			get {
				if (!TryGetValue(handle, out var v))
					throw new KeyNotFoundException(); //qq detail
				return v;
			}
			set {
				switch (handle.Kind) {
					case StreamHandle.Kind.Hash:
						_nonCollisions[handle.StreamHash] = value;
						break;
					case StreamHandle.Kind.Id:
						_collisions[handle.StreamId] = value;
						break;
					default:
						throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
				}
			}
		}

		//qq it is required that the key we use is already checked for collisions.
		//qq ok, in a nutshell my idea of mapping the streams to their metadata won't work because
		// say we have metdatas for two streams, and the stream names happen to collide, but one of the
		// streams doesn't actually exist, then the index check cant detect the collision, and one
		// streams metadata may overwrite the other.
		// put another way, this violates the requirement that we check the keys for collisions with
		// other keys before using them to store data against here, because we can only check for
		// collisions of things actually present in the log. or put a third way, only names in the log
		// can be used as keys.
		//
		// SO we can't use the stream names as keys in the map - we have to use the metadata stream names
		// will that work? should do. we will key against the metadta streams, because indeed every
		// stream that needs scavenging is associated with a metadata stream. when we scavenge a chunk we
		// can determine the metadata stream for each record we find easily, and see if it exists. i
		// think it's fine.
		public TValue this[TKey key] {
			get {
				if (!TryGetValue(key, out var v))
					throw new KeyNotFoundException(); //qq detail
				return v;
			}

			set {
				if (_isCollision(key)) {
					_collisions[key] = value;
				} else {
					_nonCollisions[_hasher.Hash(key)] = value;
				}
			}
		}

		// when a key that didn't used to be a collision, becomes a collision.
		public void NotifyCollision(TKey key) {
			var hash = _hasher.Hash(key);
			//qq make sure this works even if crashing at unopportune time, coordinating between
			// two persistent datastructures.
			if (_nonCollisions.TryRemove(hash, out var value)) {
				_collisions[key] = value;
			} else {
				//qq we are notified that the key is a collision, but we dont have any entry for it
				// so probably nothing to do
			}
		}

		//qq consider name and whether this wants to be a method, and whether in fact it should return
		// an enumerator or an enumerable
		//qq generalize so that it isn't for streams specifically
		//qq consider if we need to cover writing happening while we enumerate
		public IEnumerable<(StreamHandle<TKey> Handle, TValue Value)> Enumerate() {
			foreach (var kvp in _collisions) {
				yield return (StreamHandle.ForStreamId(kvp.Key), kvp.Value);
			}

			foreach (var kvp in _nonCollisions) {
				yield return (StreamHandle.ForHash<TKey>(kvp.Key), kvp.Value);
			}
		}
	}
}