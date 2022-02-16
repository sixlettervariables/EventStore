using System;
using EventStore.Core.DataStructures.ProbabilisticFilter;
using Xunit;

#pragma warning disable xUnit1026 // Theory methods should use all of their parameters
namespace EventStore.Core.XUnit.Tests.DataStructures.ProbabilisticFilter {
	public unsafe class BloomFilterAccessorTests {
		BloomFilterAccessor GenSut(long logicalFilterSize, BloomFilterAccessor.OnPageDirty onPageDirty = null) {
			return new BloomFilterAccessor(
				logicalFilterSize: logicalFilterSize,
				cacheLineSize: BloomFilterIntegrity.CacheLineSize,
				hashSize: BloomFilterIntegrity.HashSize,
				pageSize: BloomFilterIntegrity.PageSize,
				onPageDirty: onPageDirty ?? (pageNumber => { }),
				log: Serilog.Log.Logger);
		}

		[Fact]
		public void CorrectProperties() {
			var sut = GenSut(10 * 1024);
			Assert.Equal(BloomFilterIntegrity.CacheLineSize, sut.CacheLineSize);
			Assert.Equal(BloomFilterIntegrity.HashSize, sut.HashSize);
			Assert.Equal(BloomFilterIntegrity.PageSize, sut.PageSize);
		}

		[Theory]
		[InlineData(10_000)]
		[InlineData(10_001)]
		[InlineData(256_000_000)]
		public void CalculatesLogicalSize(int size) {
			var sut = GenSut(size);

			Assert.Equal(size, sut.LogicalFilterSize);
			Assert.Equal(size * 8, sut.LogicalFilterSizeBits);
		}

		[Theory]
		// we have a 64 bytes in the file for every 60 bytes of logical filter.
		// plus 64 bytes for the header
		// and another 64 bytes since it hits the boundary (unnecessary but change would be breaking)
		[InlineData(60 * 500, 64 * 500 + 64 + 64, "")]
		[InlineData(60 * 500 - 1, 64 * 500 + 64, "one less byte would need one less cache line")]
		[InlineData(60 * 500 + 59, 64 * 500 + 64 + 64, "adding 59 more bytes still fits")]
		[InlineData(60 * 500 + 60, 64 * 500 + 64 + 64 + 64, "but a 60th byte requires an extra cache line")]
		public void CalculatesFileSize(int size, int expected, string detail) {
			var sut = GenSut(size);
			Assert.Equal(expected, sut.FileSize);
			Assert.True(sut.FileSize % 64 == 0);
			Assert.Equal(expected / 64, sut.NumCacheLines);
		}

		[Theory]
		// we have a 8192 / 64 = 128 cachelines per page
		// and we have one cacheline per 60 bytes of logical filter
		// so we have one page for every 60 * 128 = 7680 bytes of logical filter
		// plus one more page since it hits the boundary (unnecessary but change would be breaking)
		[InlineData(30 * 7680, 30 + 1, "")]
		[InlineData(30 * 7680 - 1, 30, "one less byte would need one less page")]
		[InlineData(30 * 7680 + 7679, 30 + 1, "7679 more bytes would still fit")]
		[InlineData(30 * 7680 + 7680, 30 + 1 + 1, "but 7680 requires another page")]
		public void CalculatesNumPages(int size, int expected, string detail) {
			var sut = GenSut(size);
			Assert.Equal(expected, sut.NumPages);
		}

		[Fact]
		public void CalculatesBytePositionInFile() {
			var sut = GenSut(10_000);
			Assert.Equal(64, sut.GetBytePositionInFile(0));
			Assert.Equal(64, sut.GetBytePositionInFile(1));
			Assert.Equal(64, sut.GetBytePositionInFile(7));
			Assert.Equal(65, sut.GetBytePositionInFile(8));
			// jumps over the 4 bytes for the hash
			Assert.Equal(64 + 59, sut.GetBytePositionInFile(60 * 8 - 1));
			Assert.Equal(64 + 64, sut.GetBytePositionInFile(60 * 8));
		}

		[Fact]
		public void CalculatesPagePositionInFile() {
			var sut = GenSut(10_000);
			Assert.Equal((64, 8 * 1024), sut.GetPagePositionInFile(0));
			Assert.Equal((64 + 8 * 1024, 2496), sut.GetPagePositionInFile(1));
			Assert.Equal(sut.FileSize, 8 * 1024 + 64 + 2496);
			Assert.Throws<ArgumentOutOfRangeException>(() => sut.GetPagePositionInFile(2));
		}

		[Fact]
		public void CalculatesPagePositionInFileLarge() {
			var sut = GenSut(4_000_000_000);
			Assert.Equal((64, 8 * 1024), sut.GetPagePositionInFile(0));
			Assert.Equal((4_266_664_000, 2752), sut.GetPagePositionInFile(520_833));
		}

		[Fact]
		public void CalculatesPageNumber() {
			var sut = GenSut(10_000);
			Assert.Equal(0, sut.GetPageNumber(64));
			Assert.Equal(0, sut.GetPageNumber(64 + 8 * 1024 - 1));
			Assert.Equal(1, sut.GetPageNumber(64 + 8 * 1024));
			Assert.Equal(1, sut.GetPageNumber(sut.FileSize));
		}

		[Fact]
		public void CanSetAndTestBits() {
			var dirtyPage = -1L;
			var sut = GenSut(10_000, pageNumber => dirtyPage = pageNumber);
			using var mem = new AlignedMemory(sut.FileSize, 64);
			sut.Pointer = mem.Pointer;
			sut.FillWithZeros();

			Assert.False(sut.IsBitSet(0));
			sut.SetBit(0);
			Assert.True(sut.IsBitSet(0));
			Assert.False(sut.IsBitSet(1));
			Assert.Equal(0, dirtyPage);
		}

		[Fact]
		public void CanSetAndTestBitsLargeFile() {
			var dirtyPage = -1L;
			var sut = GenSut(4_000_000_000, pageNumber => dirtyPage = pageNumber);
			using var mem = new AlignedMemory(sut.FileSize, 64);
			sut.Pointer = mem.Pointer;

			sut.SetBit(0);
			Assert.True(sut.IsBitSet(0));
			Assert.Equal(0, dirtyPage);

			sut.SetBit(4_000_000_000L * 8);
			Assert.True(sut.IsBitSet(4_000_000_000L * 8));
			Assert.Equal(520_833, dirtyPage);
		}

		[Fact]
		public void CanVerifySuccessfully() {
			var sut = GenSut(10_000);
			using var mem = new AlignedMemory(sut.FileSize, 64);
			sut.Pointer = mem.Pointer;
			sut.FillWithZeros();

			sut.SetBit(0);
			sut.Verify(0, 0);
		}

		[Fact]
		public void CanFailToVerify() {
			var sut = GenSut(10_000);
			using var mem = new AlignedMemory(sut.FileSize, 64);
			sut.Pointer = mem.Pointer;
			sut.FillWithZeros();

			ref var b = ref sut.ReadBytes(bytePositionInFile: 64, count: 1)[0];
			Assert.False(b.IsBitSet(0));
			sut.SetBit(0); // set the bit through the filter
			Assert.True(b.IsBitSet(0)); // affected our reference

			sut.Verify(0, 0);

			b = b.SetBit(2); // setting the bit directly not through the accessor
			b = b.SetBit(3);

			// corruption doesn't meet 5% threshold
			sut.Verify(corruptionRebuildCount: 0, corruptionThreshold: 5);

			Assert.Throws<CorruptedHashException>(() => {
				// corruption does meet 0% threshold
				sut.Verify(0, 0);
			});

		}

		[Fact]
		public void ComplainsAboutUnalignedPointer() {
			var sut = GenSut(10_000);
			var ex = Assert.Throws<InvalidOperationException>(() => sut.Pointer = (byte*)123);
			Assert.Equal("Pointer 123 is not aligned to a cacheline (64)", ex.Message);
		}
	}
}
