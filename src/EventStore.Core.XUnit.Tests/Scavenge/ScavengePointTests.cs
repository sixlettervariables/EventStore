using System;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ScavengePointTests {
		[Theory]
		[InlineData(100, 0, 0)]
		[InlineData(100, 99, 0)]
		[InlineData(100, 100, 100)]
		[InlineData(100, 199, 100)]
		[InlineData(100, 200, 200)]
		public void calculates_uptoposition_correctly(
			long chunkSize,
			long scavengePointPosition,
			long expectedUpToPosition) {

			var sp = ScavengePoint.CreateForLogPosition(
				chunkSize: chunkSize,
				scavengePointLogPosition: scavengePointPosition,
				eventNumber: 5,
				effectiveNow: DateTime.UtcNow);

			Assert.Equal(expectedUpToPosition, sp.UpToPosition);
		}
	}
}
