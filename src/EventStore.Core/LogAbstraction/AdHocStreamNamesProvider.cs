﻿using System;
using EventStore.Core.Index;
using EventStore.Core.Services.Storage.ReaderIndex;

namespace EventStore.Core.LogAbstraction {
	// mechanism to delay construction of StreamNames and SystemStreams until the IndexReader is available
	public class AdHocStreamNamesProvider<TStreamId> : IStreamNamesProvider<TStreamId> {
		private readonly Func<IIndexReader<TStreamId>, (ISystemStreamLookup<TStreamId>, INameLookup<TStreamId>, INameExistenceFilterInitializer)> _setReader;
		private readonly Func<ITableIndex, INameExistenceFilterInitializer> _setTableIndex;

		private ISystemStreamLookup<TStreamId> _systemStreams;
		private INameLookup<TStreamId> _streamNames;
		private INameExistenceFilterInitializer _streamExistenceFilterInitializer;

		public AdHocStreamNamesProvider(
			Func<(ISystemStreamLookup<TStreamId>, INameLookup<TStreamId>, INameExistenceFilterInitializer)> init,
			Func<IIndexReader<TStreamId>, (ISystemStreamLookup<TStreamId>, INameLookup<TStreamId>, INameExistenceFilterInitializer)> setReader,
			Func<ITableIndex, INameExistenceFilterInitializer> setTableIndex) {
			(_systemStreams, _streamNames, _streamExistenceFilterInitializer) = init();
			_setReader = setReader;
			_setTableIndex = setTableIndex;
		}

		public INameLookup<TStreamId> StreamNames =>
			_streamNames ?? throw new InvalidOperationException("Call SetReader first");

		public ISystemStreamLookup<TStreamId> SystemStreams =>
			_systemStreams ?? throw new InvalidOperationException("Call SetReader first");

		public INameExistenceFilterInitializer StreamExistenceFilterInitializer =>
			_streamExistenceFilterInitializer ?? throw new InvalidOperationException("Call SetReader or SetTableIndex first");

		//qq hmm
		public void SetReader(IIndexReader<TStreamId> reader) {
			var (systemStreams, streamNames, streamExistenceFilterInitializer) = _setReader(reader);
			_systemStreams = systemStreams ?? _systemStreams;
			_streamNames = streamNames ?? _streamNames;
			_streamExistenceFilterInitializer = streamExistenceFilterInitializer ?? _streamExistenceFilterInitializer;
		}

		public void SetTableIndex(ITableIndex tableIndex) {
			var streamExistenceFilterInitializer = _setTableIndex(tableIndex);
			_streamExistenceFilterInitializer = streamExistenceFilterInitializer ?? _streamExistenceFilterInitializer;
		}
	}
}