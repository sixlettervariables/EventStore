using System;
using EventStore.Core.Caching;
using EventStore.Core.Data;
using NUnit.Framework;
using MetadataCached = EventStore.Core.Services.Storage.ReaderIndex.IndexBackend<string>.MetadataCached;

namespace EventStore.Core.Tests.Caching {
	public class MetadataCachedTests {
		[Test]
		public void size_is_measured_correctly() {
			var mem = MemUsage<MetadataCached[]>.Calculate(() =>
				new MetadataCached[] { // we need an array to force an allocation
					new(0, new StreamMetadata(
						maxCount: 10,
						maxAge: new TimeSpan(10),
						truncateBefore: null,
						tempStream: false,
						cacheControl: null,
						acl: new StreamAcl(
							new[] {new string(' ', 1), new string(' ', 2)},
							new[] {new string(' ', 0)},
							null,
							null,
							new[] {new string(' ', 1), new string(' ', 3), new string(' ', 3), new string(' ', 7)}
						)
					))
				}, out var metadata);

			Assert.AreEqual(mem,metadata[0].Size + MemSizer.ArraySize);
		}
	}
}
