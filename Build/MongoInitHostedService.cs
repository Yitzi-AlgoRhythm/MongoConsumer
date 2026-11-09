using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Hosting;
using MongoConsumer.DbCreation;
using MongoLib.Documents;
using MongoLib.Init;

namespace MongoConsumer.Build
{
    public sealed class MongoInitHostedService : IHostedService
    {
        private readonly MongoDbInitializer _initializer;
        private readonly FieldCatalogSeedProvider _seedProvider;

        public MongoInitHostedService(MongoDbInitializer initializer, FieldCatalogSeedProvider seedProvider)
        {
            _initializer = initializer;
            _seedProvider = seedProvider;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            IEnumerable<FieldCatalogDocument> seed = _seedProvider.GetSeed();
            await _initializer.EnsureCreatedAsync(seed, cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
