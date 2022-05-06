using System;
using System.IO;
using System.Threading.Tasks;
using EventStore.Core.Tests;
using Xunit;

namespace EventStore.Core.XUnit.Tests {
	public class DirectoryFixture<T> : IAsyncLifetime {
		private readonly bool _deleteDir;
		public string Directory;

		public DirectoryFixture(bool deleteDir=true) {
			_deleteDir = deleteDir;
			var typeName = typeof(T).Name.Length > 30 ? typeof(T).Name.Substring(0, 30) : typeof(T).Name;
			Directory = Path.Combine(Path.GetTempPath(), string.Format("ESX-{0}-{1}", Guid.NewGuid(), typeName));
			System.IO.Directory.CreateDirectory(Directory);
		}

		~DirectoryFixture() {
			if (_deleteDir) {
				DirectoryDeleter.TryForceDeleteDirectoryAsync(Directory).Wait();	
			}
		}

		public string GetTempFilePath() {
			return Path.Combine(Directory, string.Format("{0}-{1}", Guid.NewGuid(), typeof(T).FullName));
		}

		public string GetFilePathFor(string fileName) {
			return Path.Combine(Directory, fileName);
		}

		public Task InitializeAsync() {
			return Task.CompletedTask;
		}

		public async Task DisposeAsync() {
			if (_deleteDir) {
				await DirectoryDeleter.TryForceDeleteDirectoryAsync(Directory);	
			}
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
			GC.SuppressFinalize(this);
#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
		}
	}
}
