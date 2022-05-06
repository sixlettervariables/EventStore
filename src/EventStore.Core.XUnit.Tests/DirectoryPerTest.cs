using System.Threading.Tasks;
using Xunit;

namespace EventStore.Core.XUnit.Tests {
	public class DirectoryPerTest<T> : IAsyncLifetime {
		protected DirectoryFixture<T> Fixture { get; }

		public DirectoryPerTest(bool deleteDir=true) {
			Fixture = new DirectoryFixture<T>(deleteDir);			
		}
		
		public async Task InitializeAsync() {
			await Fixture.InitializeAsync();
		}

		public async Task DisposeAsync() {
			await Fixture.DisposeAsync();
		}
	}
}
