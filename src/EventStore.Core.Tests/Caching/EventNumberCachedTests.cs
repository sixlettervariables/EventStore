using EventStore.Core.Caching;
using NUnit.Framework;
using EventNumberCached = EventStore.Core.Services.Storage.ReaderIndex.IndexBackend<string>.EventNumberCached;

namespace EventStore.Core.Tests.Caching {
	public class EventNumberCachedTests {
		[Test]
		public void size_is_measured_correctly() {
			var mem = MemUsage<EventNumberCached[]>.Calculate(() =>
				new EventNumberCached[] { // we need an array to force an allocation
					new(10, 123)
				}, out _);

			Assert.AreEqual(mem,EventNumberCached.Size + MemSizer.ArraySize);
		}
	}
}
