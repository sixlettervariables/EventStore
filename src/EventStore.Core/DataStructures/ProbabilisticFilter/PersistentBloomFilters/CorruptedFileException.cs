using System;

namespace EventStore.Core.DataStructures.ProbabilisticFilter.PersistentBloomFilters {
	public class CorruptedFileException : Exception {
		public CorruptedFileException(string error, Exception inner = null) : base(error, inner) { }
	}
}
