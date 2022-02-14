using System;

namespace EventStore.Core.DataStructures.ProbabilisticFilter.PersistentBloomFilters {
	public class CorruptedHashException : Exception {
		public CorruptedHashException(int rebuildCount, string error) : base(error) {
			RebuildCount = rebuildCount;
		}

		public int RebuildCount { get; }
	}
}
