using System;
using System.Collections.Generic;
using System.Text;
using ParameterDataLib;

namespace MongoConsumer.Pipeline
{
    public class MongoPipeline : IPipeline
    {
        public Task Completion;

        public MongoPipeline()
        {

        }

        public void Complete()
        {
        }

        public Task StartAsync(ParameterData[] batch, CancellationToken token)
        {
        }
    }
}
