using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	// The checkpoint stores which scavengepoint we are processing and where we are up to with it.

	public abstract class ScavengeCheckpoint {
		protected ScavengeCheckpoint(ScavengePoint scavengePoint) {
			ScavengePoint = scavengePoint;
		}

		public ScavengePoint ScavengePoint { get; }

		public class Accumulating : ScavengeCheckpoint {
			// Accumulating with null doneLogicalChunkNumber means we are accumulating now but havent
			// accumulated anything.
			public Accumulating(ScavengePoint scavengePoint, int? doneLogicalChunkNumber)
				: base(scavengePoint) {
				DoneLogicalChunkNumber = doneLogicalChunkNumber;
			}

			public int? DoneLogicalChunkNumber { get; }
		}

		public class Calculating<TStreamId> : ScavengeCheckpoint {
			public Calculating(ScavengePoint scavengePoint, StreamHandle<TStreamId> doneStreamHandle)
				: base(scavengePoint) {
				DoneStreamHandle = doneStreamHandle;
			}

			public StreamHandle<TStreamId> DoneStreamHandle { get; }
		}

		public class ExecutingChunks : ScavengeCheckpoint {
			public int? DoneLogicalChunkNumber { get; }

			public ExecutingChunks(ScavengePoint scavengePoint, int? doneLogicalChunkNumber)
				: base(scavengePoint) {
				DoneLogicalChunkNumber = doneLogicalChunkNumber;
			}
		}

		public class ExecutingIndex : ScavengeCheckpoint {
			public ExecutingIndex(ScavengePoint scavengePoint)
				: base(scavengePoint) {
			}
		}

		public class Merging : ScavengeCheckpoint {
			public Merging(ScavengePoint scavengePoint)
				: base(scavengePoint) {
			}
		}

		//qq name. if this is a phase at all.
		public class Tidying : ScavengeCheckpoint {
			public Tidying(ScavengePoint scavengePoint)
				: base(scavengePoint) {
			}
		}

		public class Done : ScavengeCheckpoint {
			public Done(ScavengePoint scavengePoint)
				: base(scavengePoint) {
			}
		}
	}
}
