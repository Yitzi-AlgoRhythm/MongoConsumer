using System;
using System.Collections.Generic;
using System.Text;
using BaselineBuilder.ICDHandling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoConsumer.Config;
using MongoConsumer.DbCreation;
using MongoLib;

namespace MongoConsumer.Build
{
    public static class HostBuilderExtensions
    {
        public static void AddAppServices(this HostApplicationBuilder builder)
        {
            builder.Services.Configure<KafkaConsumerConfig>
                (builder.Configuration.GetSection(nameof(KafkaConsumerConfig)));

            builder.Services.AddMongoLib(builder.Configuration.GetSection("MongoSettings"));

            builder.Services.AddSingleton<IcdDeserializer>();

            builder.Services.AddSingleton<FieldCatalogSeedProvider>();

            builder.Services.AddHostedService<MongoInitHostedService>();
            builder.Services.AddHostedService<KafkaConsumerService>();
        }
    }
}
