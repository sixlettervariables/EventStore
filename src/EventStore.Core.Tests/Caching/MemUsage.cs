using System;

namespace EventStore.Core.Tests.Caching {
	public class MemUsage<T> {
		public static long Calculate(Func<T> createObject, out T newObject) {
			var startMem = GC.GetAllocatedBytesForCurrentThread();
			newObject = createObject();
			var endMem = GC.GetAllocatedBytesForCurrentThread();
			return endMem - startMem;
		}
	}
}
