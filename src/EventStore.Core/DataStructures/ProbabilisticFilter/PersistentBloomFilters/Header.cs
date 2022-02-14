using System.Runtime.InteropServices;

namespace EventStore.Core.DataStructures.ProbabilisticFilter.PersistentBloomFilters {
	[StructLayout(LayoutKind.Explicit, Size = Size, Pack = 1)]
	public struct Header {
		internal const byte CurrentVersion = 1;
		internal const int Size = 16;

		[FieldOffset(0)] private byte _version;
		[FieldOffset(4)] private int _corruptionRebuildCount;
		[FieldOffset(8)] private long _numBits;


		public byte Version {
			get => _version;
			set => _version = value;
		}

		public int CorruptionRebuildCount {
			get => _corruptionRebuildCount;
			set => _corruptionRebuildCount = value;
		}

		public long NumBits {
			get => _numBits;
			set => _numBits = value;
		}
	}
}
