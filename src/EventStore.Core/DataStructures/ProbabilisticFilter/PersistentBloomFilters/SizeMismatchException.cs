using System;

namespace EventStore.Core.DataStructures.ProbabilisticFilter.PersistentBloomFilters {
	public class SizeMismatchException : Exception {
		public SizeMismatchException(string error) : base(error) { }
	}
}
