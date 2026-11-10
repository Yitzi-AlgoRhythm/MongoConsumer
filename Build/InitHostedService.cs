using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Hosting;
using MongoConsumer.DbCreation;
using MongoConsumer.Pipeline;
using MongoLib.Documents;
using MongoLib.Init;

namespace MongoConsumer.Build
{
    public sealed class InitHostedService : IHostedService
    {
        private readonly MongoDbInitializer _initializer;
        private readonly FieldCatalogSeedProvider _seedProvider;
        private readonly TelemetryPipeline _pipeline;

        public InitHostedService
        (
            MongoDbInitializer initializer, 
            FieldCatalogSeedProvider seedProvider,
            TelemetryPipeline pipeline
        )
        {
            _initializer = initializer;
            _seedProvider = seedProvider;
            _pipeline = pipeline;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            IEnumerable<FieldCatalogDocument> seed = _seedProvider.GetSeed();
            await _initializer.EnsureCreatedAsync(seed, cancellationToken);

            await _pipeline.InitFlightAssignmentAsync(cancellationToken);
            await _pipeline.InitCollectAsync(cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
