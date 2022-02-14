using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using EventStore.Common.Utils;
using Serilog;

namespace EventStore.Core.DataStructures.ProbabilisticFilter.PersistentBloomFilters {
	//qqq step through the 4 cases considering whether it is ok in the wrong-size case.
	// - fsync create
	// - fsync existing
	// - msync create
	// - msync existing


	// testing
	// - new classes unit tests
	// - prolly integration tests with both strategies
	// - check if we unalign the memory it goes slower
	// compatibility with the previous filter files
	// perf test
	// power pull test
	// time how long fillwithzeroes takes for a max size file.

	public unsafe class FileStreamPersistence : IPersistenceStrategy {
		protected static readonly ILogger Log = Serilog.Log.ForContext<FileStreamPersistence>();

		// We synchronize access to _dirtyPageBitmap because
		// Flush can be called at the same time as OnPageDirty (we dont pause writes into the
		// bloom filter while it is flushing). And they both write to the bitmap (to unset and
		// set bits respectively)
		// This lock is often obtained while holding the write lock to the filter
		// therefore don't make any non-trivial external calls while holding this lock
		private readonly object _bitmapLock = new();
		private readonly long _logicalFilterSize;
		private readonly string _path;
		private AlignedMemory _bloomFilterMemory;
		private AlignedMemory _dirtyPageBitmap;
		private bool _disposed;

		public FileStreamPersistence(long size, string path, bool create) {
			Ensure.NotNull(path, nameof(path));
			_logicalFilterSize = size;
			_path = path;
			Create = create;
		}

		public BloomFilterAccessor DataAccessor { get; private set; }
		public bool Create { get; }

		public void Init() {
			DataAccessor = new BloomFilterAccessor(
				logicalFilterSize: _logicalFilterSize,
				cacheLineSize: BloomFilterIntegrity.CacheLineSize,
				hashSize: BloomFilterIntegrity.HashSize,
				pageSize: BloomFilterIntegrity.PageSize,
				onPageDirty: OnPageDirty,
				log: Log);

			// dirtypages: one bit per page, but pad to the nearest cacheline boundary
			var numBits = DataAccessor.NumPages;
			var numBitsPadded = numBits.RoundUpToMultipleOf(BloomFilterIntegrity.CacheLineSize * 8);
			_dirtyPageBitmap = new AlignedMemory(
				size: numBitsPadded / 8,
				alignTo: BloomFilterIntegrity.CacheLineSize);
			_dirtyPageBitmap.AsSpan().Clear(); // alignedmemory isn't initialized otherwise

			// main filter:
			_bloomFilterMemory = new AlignedMemory(
				size: new IntPtr(DataAccessor.FileSize),
				alignTo: BloomFilterIntegrity.CacheLineSize);
			DataAccessor.Pointer = _bloomFilterMemory.Pointer;

			// initialize the aligned memory
			if (Create) {
				DataAccessor.FillWithZeros();
			} else {
				// load the whole filter into memory for rapid access
				BulkLoadExisting();
			}
		}

		private void BulkLoadExisting() {
			Log.Information(
				"Reading persisted bloom filter {path} of size {size:N0} bytes into memory...",
				_path,
				DataAccessor.FileSize);

			var sw = Stopwatch.StartNew();

			using var bulkFileStream = new FileStream(
				_path,
				FileMode.Open,
				FileAccess.Read,
				FileShare.ReadWrite,
				bufferSize: 65_536,
				options: FileOptions.SequentialScan);

			//qq reliable check?
			if (bulkFileStream.Length != DataAccessor.FileSize)
				throw new SizeMismatchException(
					$"The expected file size ({DataAccessor.FileSize:N0}) does not match " +
					$"the actual file size ({bulkFileStream.Length:N0}) of file {_path}");

			var bytesToRead = DataAccessor.FileSize;
			var bytesRead = 0L;
			while (bytesToRead > 0) {
				var bytesToReadInBlock = bytesToRead > int.MaxValue
					? int.MaxValue
					: (int)bytesToRead;

				bulkFileStream.Read(new Span<byte>(DataAccessor.Pointer + bytesRead, bytesToReadInBlock));

				bytesRead += bytesToReadInBlock;
				bytesToRead -= bytesToReadInBlock;
			}

			var elapsed = sw.Elapsed;
			var fileSizeMb = DataAccessor.FileSize / 1000 / 1000;
			var megaBytesPerSecond = fileSizeMb / elapsed.TotalSeconds;
			Log.Information(
				"Read persisted bloom filter {path} into memory. Took {elapsed}. {megaBytesPerSecond} MB/s",
				_path,
				elapsed,
				megaBytesPerSecond);
		}

		private void OnPageDirty(long pageNumber) {
			lock (_bitmapLock) {
				ThrowIfDisposed();
				var byteIndex = (int)(pageNumber / 8);
				var bitIndex = pageNumber % 8;
				ref var byteValue = ref _dirtyPageBitmap.AsSpan()[byteIndex];
				byteValue = byteValue.SetBit(bitIndex);
			}
		}

		public void Flush() {
			using var fileStream = new FileStream(
				_path,
				FileMode.OpenOrCreate,
				FileAccess.ReadWrite,
				FileShare.ReadWrite,
				bufferSize: DataAccessor.PageSize);

			fileStream.SetLength(DataAccessor.FileSize);

			Span<byte> localCacheLine = stackalloc byte[BloomFilterIntegrity.CacheLineSize];
			localCacheLine.Clear();

			var pageNumber = 0;

			for (var remaining = _dirtyPageBitmap.AsSpan();
				remaining.Length > 0;
				remaining = remaining[BloomFilterIntegrity.CacheLineSize..]) {

				lock (_bitmapLock) {
					ThrowIfDisposed();
					var cacheLine = remaining[..BloomFilterIntegrity.CacheLineSize];
					cacheLine.CopyTo(localCacheLine);
					cacheLine.Clear();
				}

				foreach (var @byte in localCacheLine) {
					for (var bitOffset = 0; bitOffset < 8; bitOffset++) {
						if (@byte.IsBitSet(bitOffset)) {
							WritePage(pageNumber, fileStream);
							//qq configurable? sleep to control the speed
						}

						pageNumber++;
						if (pageNumber == DataAccessor.NumPages)
							goto Done;
					}
				}
			}

			Done:
			fileStream.FlushToDisk();
		}

		private void WritePage(int pageNumber, FileStream fileStream) {
			var (fileOffset, pageSize) = DataAccessor.GetPagePositionInFile(pageNumber);
			fileStream.Seek(offset: fileOffset, SeekOrigin.Begin);
			fileStream.Write(DataAccessor.ReadBytes(fileOffset, pageSize));
		}

		public Header ReadHeader() {
			try {
				using var fileStream = new FileStream(
					_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);

				//read the version first
				fileStream.Seek(offset: 0, SeekOrigin.Begin);
				byte version = (byte)fileStream.ReadByte();
				if (version != Header.CurrentVersion) {
					throw new CorruptedFileException($"Unsupported version: {version}");
				}

				//then the full header
				var headerBytes = new byte[Header.Size].AsSpan();

				fileStream.Seek(offset: 0, SeekOrigin.Begin);
				var read = fileStream.Read(headerBytes);
				if (read != Header.Size) {
					throw new CorruptedFileException(
						$"File header size ({read} bytes) does not match expected header size ({Header.Size} bytes)");
				}

				return MemoryMarshal.AsRef<Header>(headerBytes);
			} catch (Exception exc) when (exc is not CorruptedFileException) {
				throw new CorruptedFileException("Failed to read the header", exc);
			}
		}

		public void WriteHeader(Header header) {
			using var fileStream = new FileStream(
				_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
			var span = MemoryMarshal.CreateReadOnlySpan(ref header, 1);
			var headerBytes = MemoryMarshal.Cast<Header, byte>(span);
			fileStream.Seek(offset: 0, SeekOrigin.Begin);
			fileStream.Write(headerBytes);
			fileStream.FlushToDisk();
		}

		private void ThrowIfDisposed() {
			if (_disposed) {
				throw new ObjectDisposedException(nameof(FileStreamPersistence));
			}
		}

		public void Dispose() {
			lock (_bitmapLock) {
				if (_disposed)
					return;

				_disposed = true;

				if (DataAccessor is not null)
					DataAccessor.Pointer = default;

				_bloomFilterMemory?.Dispose();
				_dirtyPageBitmap?.Dispose();
			}
		}
	}
}
