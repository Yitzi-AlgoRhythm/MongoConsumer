using System;
using System.Collections.Generic;
using System.Text;
using ParameterDataLib;

namespace MongoConsumer.Pipeline
{
    public interface IPipeline
    {
        Task StartAsync(ParameterData[] batch, CancellationToken token);

        void Complete();

        Task Completion { get; }
    }
}
