using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MongoConsumer.Config;
using MongoConsumer.Pipeline;
using ParameterDataLib;

namespace MongoConsumer.Build
{
    public class KafkaConsumerService : BackgroundService
    {
        private static readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(2);

        private readonly IPipeline _pipeline;
        private readonly KafkaConsumerConfig _config;
        private readonly IHostApplicationLifetime _lifetime;

        public KafkaConsumerService(
            IPipeline pipeline,
            IOptions<KafkaConsumerConfig> options,
            IHostApplicationLifetime lifetime)
        {
            _pipeline = pipeline;
            _config = options.Value;
            _lifetime = lifetime;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => RunAsync(stoppingToken), stoppingToken);
        }

        private async Task RunAsync(CancellationToken token)
        {
            ConsumerConfig consumerConfig = new()
            {
                BootstrapServers = _config.BootstrapServers,
                GroupId = _config.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

            try
            {
                consumer.Subscribe(_config.TopicName);

                while (!token.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;

                    try
                    {
                        result = consumer.Consume(token);
                    }
                    catch (ConsumeException ex) when (!ex.Error.IsFatal)
                    {
                        Console.WriteLine($"Kafka consume error: {ex.Error.Reason}");
                        Console.WriteLine(ex);
                        await Task.Delay(_retryDelay, token);
                        continue;
                    }

                    if (result?.Message?.Value is null)
                    {
                        continue;
                    }

                    ParameterData[]? batch;

                    try
                    {
                        batch = JsonSerializer.Deserialize<ParameterData[]>(result.Message.Value);
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine($"Skipping malformed message at {result.TopicPartitionOffset}");
                        Console.WriteLine(ex);
                        continue;
                    }

                    if (batch is null || batch.Length == 0)
                    {
                        Console.WriteLine($"Skipping empty message at {result.TopicPartitionOffset}");
                        continue;
                    }

                    await _pipeline.StartAsync(batch, token);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Normal shutdown.
            }
            catch (Exception ex)
            {
                Console.WriteLine("Kafka consume loop failed; stopping the application.");
                Console.WriteLine(ex);
                Environment.ExitCode = 1;
                _lifetime.StopApplication();
            }
            finally
            {
                try
                {
                    _pipeline.Complete();
                    await _pipeline.Completion;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Pipeline failed while draining.");
                    Console.WriteLine(ex);
                    Environment.ExitCode = 1;
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}