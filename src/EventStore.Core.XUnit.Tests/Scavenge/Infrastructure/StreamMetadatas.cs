﻿using System;
using EventStore.Core.Data;
using EventStore.Core.Services;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class StreamMetadatas {
		public static StreamMetadata TruncateBefore1 { get; } = new StreamMetadata(truncateBefore: 1);
		public static StreamMetadata TruncateBefore2 { get; } = new StreamMetadata(truncateBefore: 2);
		public static StreamMetadata TruncateBefore3 { get; } = new StreamMetadata(truncateBefore: 3);
		public static StreamMetadata TruncateBefore4 { get; } = new StreamMetadata(truncateBefore: 4);

		public static StreamMetadata MaxCount1 { get; } = new StreamMetadata(maxCount: 1);
		public static StreamMetadata MaxCount2 { get; } = new StreamMetadata(maxCount: 2);
		public static StreamMetadata MaxCount3 { get; } = new StreamMetadata(maxCount: 3);
		public static StreamMetadata MaxCount4 { get; } = new StreamMetadata(maxCount: 4);

		public static StreamMetadata MaxAgeMetadata { get; } =
			new StreamMetadata(maxAge: TimeSpan.FromDays(2));

		public static StreamMetadata SoftDelete { get; } =
			new StreamMetadata(truncateBefore: EventNumber.DeletedStream);

		public static DateTime EffectiveNow { get; } = new DateTime(2022, 1, 5, 00, 00, 00);
		public static DateTime Expired { get; } = EffectiveNow - TimeSpan.FromDays(3);
		public static DateTime Active { get; } = EffectiveNow - TimeSpan.FromDays(1);

		public static Rec ScavengePointRec(int transaction, int threshold = 0) => Rec.Prepare(
			transaction: transaction,
			stream: SystemStreams.ScavengePointsStream,
			eventType: SystemEventTypes.ScavengePoint,
			timestamp: EffectiveNow,
			data: new ScavengePointPayload {
				Threshold = threshold,
			}.ToJsonBytes());

		public static ScavengePoint ScavengePoint(int chunk, long eventNumber) => new ScavengePoint(
			//qq 1024*1024 is the chunk size, want less magic
			position: 1024 * 1024 * chunk,
			eventNumber: eventNumber,
			effectiveNow: EffectiveNow,
			threshold: 0);
	}
}