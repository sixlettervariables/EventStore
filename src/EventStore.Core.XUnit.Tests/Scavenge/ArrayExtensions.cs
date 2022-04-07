using System.Linq;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public static class ArrayExtensions {
		public static T[] KeepIndexes<T>(this T[] self, params int[] indexes) {
			foreach (var i in indexes) {
				Assert.True(i < self.Length, $"error in test: index {i} does not exist");
			}

			return self.Where((x, i) => indexes.Contains(i)).ToArray();
		}
	}

}
