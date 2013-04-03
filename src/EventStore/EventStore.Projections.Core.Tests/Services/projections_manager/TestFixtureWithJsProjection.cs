// Copyright (c) 2012, Event Store LLP
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
// Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
// Neither the name of the Event Store LLP nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// 

using System;
using System.Collections.Generic;
using EventStore.Core.Tests;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.projections_manager
{
    public abstract class TestFixtureWithJsProjection
    {
        private ProjectionStateHandlerFactory _stateHandlerFactory;
        protected IProjectionStateHandler _stateHandler;
        protected List<string> _logged;
        protected string _projection;
        protected string _state = null;
        protected SourceRecorder _source;

        [SetUp]
        public void Setup()
        {
            _state = null;
            _projection = null;
            Given();
            _logged = new List<string>();
            _stateHandlerFactory = new ProjectionStateHandlerFactory();
            _stateHandler = _stateHandlerFactory.Create(
                "JS", _projection, logger: s =>
                    {
                        if (!s.StartsWith("P:")) _logged.Add(s);
                        else Console.WriteLine(s);
                    }); // skip prelude debug output
            _source = new SourceRecorder();
            _stateHandler.ConfigureSourceProcessingStrategy(_source);
            if (_state != null)
                _stateHandler.Load(_state);
            else
                _stateHandler.Initialize();
        }

        protected abstract void Given();

        [TearDown]
        public void Teardown()
        {
            if (_stateHandler != null)
                _stateHandler.Dispose();
            _stateHandler = null;
            GC.Collect(2, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();
        }
    }

    public class SourceRecorder : QuerySourceProcessingStrategyBuilder
    {
        public bool AllStreams
        {
            get { return _allStreams; }
        }

        public List<string> Categories
        {
            get { return _categories; }
        }

        public List<string> Streams
        {
            get { return _streams; }
        }

        public bool AllEvents1
        {
            get { return _allEvents; }
        }

        public List<string> Events
        {
            get { return _events; }
        }

        public bool ByStream
        {
            get { return _byStream; }
        }

        public bool ByCustomParititions
        {
            get { return _byCustomPartitions; }
        }

        public bool DefinesStateTransform
        {
            get { return _definesStateTransform; }
        }

        public QuerySourceOptions Options 
        {
            get { return _options; }
        }
    }
}
