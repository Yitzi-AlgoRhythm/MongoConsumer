using Microsoft.Extensions.Hosting;
using MongoConsumer.Build;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.AddAppServices();

IHost host = builder.Build();

host.Run();