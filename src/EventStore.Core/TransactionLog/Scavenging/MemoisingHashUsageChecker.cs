using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class MemoisingHashUsageChecker<TStreamId> : IHashUsageChecker<TStreamId> {
		//qq irl this would be a lru cache.
		//qq see comment above InMemoryAccumulator.Accumulate(EventRecord) for another option for how
		// the cache could work
		private readonly Dictionary<ulong, TStreamId> _cache = new Dictionary<ulong, TStreamId>();
		private readonly IHashUsageChecker<TStreamId> _wrapped;

		public MemoisingHashUsageChecker(IHashUsageChecker<TStreamId> wrapped) {
			_wrapped = wrapped;
		}

		public bool HashInUseBefore(
			TStreamId streamId,
			ulong hash,
			long position,
			out TStreamId hashUser) {

			if (_cache.TryGetValue(hash, out hashUser))
				return true;

			if (_wrapped.HashInUseBefore(streamId, hash, position, out hashUser)) {
				_cache[hash] = hashUser;
				return true;
			}

			// we can cache this entry because the next time this method is called
			// the position will definitely be greater than it is this time
			// so there is no danger of returning this result when it should have
			// been excluded by the position filter.
			_cache[hash] = streamId;
			hashUser = default;
			return false;
		}
	}
}
