using System;
using System.Collections.Generic;
using System.Text;
using BaselineBuilder.ICDHandling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoConsumer.Config;
using MongoConsumer.DbCreation;
using MongoConsumer.Pipeline;
using MongoConsumer.Pipeline.Stages;
using MongoLib;
using ThpUtils.Naming;

namespace MongoConsumer.Build
{
    public static class HostBuilderExtensions
    {
        public static void AddAppServices(this HostApplicationBuilder builder)
        {
            builder.Services.Configure<KafkaConsumerConfig>
                (builder.Configuration.GetSection(nameof(KafkaConsumerConfig)));

            builder.Services.AddMongoLib(builder.Configuration);

            builder.Services.AddNaming(builder.Configuration);

            builder.Services.AddSingleton<IcdDeserializer>();

            builder.Services.AddSingleton<FieldCatalogSeedProvider>();

            builder.Services.AddSingleton<FlightAssignmentStage>();
            builder.Services.AddSingleton<CollectStage>();
            builder.Services.AddSingleton<WriteStage>();

            builder.Services.AddSingleton<TelemetryPipeline>();

            builder.Services.AddHostedService<InitHostedService>();
            builder.Services.AddHostedService<RuntimeService>();
        }
    }
}
