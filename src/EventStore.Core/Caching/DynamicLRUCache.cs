using System;
using System.Threading;
using EventStore.Core.DataStructures;

namespace EventStore.Core.Caching {
	public class DynamicLRUCache<TKey, TValue> : LRUCache<TKey, TValue>, IDynamicLRUCache<TKey, TValue> {
		public DynamicLRUCache(long capacity, Func<TKey, TValue, int> calculateItemSize = null)
			: base(capacity, calculateItemSize) { }

		public void Resize(long capacity, out int removedCount, out long removedSize) {
			Interlocked.Exchange(ref _capacity, capacity);
			EnsureCapacity(out removedCount, out removedSize);
		}
	}
}
