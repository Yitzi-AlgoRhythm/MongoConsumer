using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoConsumer.Config;

namespace MongoConsumer.Build
{
    public static class HostBuilderExtensions
    {
        public static void AddAppServices(this HostApplicationBuilder builder)
        {
            builder.Services.Configure<KafkaConsumerConfig>
                (builder.Configuration.GetSection(nameof(KafkaConsumerConfig)));

            builder.Services.AddHostedService<KafkaConsumerService>();
        }
    }
}
