using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MongoConsumer.Config;
using MongoConsumer.Pipeline;
using MongoConsumer.Pipeline.Data;
using ParameterDataLib;

namespace MongoConsumer.Build
{
    public class RuntimeService : BackgroundService
    {
        private static readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(2);

        private readonly TelemetryPipeline _pipeline;
        private readonly KafkaConsumerConfig _config;
        private readonly IHostApplicationLifetime _lifetime;

        public RuntimeService
        (
            TelemetryPipeline pipeline,
            IOptions<KafkaConsumerConfig> options,
            IHostApplicationLifetime lifetime
        )
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
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false
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
                        consumer.StoreOffset(result);
                        continue;
                    }

                    if (batch is null || batch.Length == 0)
                    {
                        Console.WriteLine($"Skipping empty message at {result.TopicPartitionOffset}");
                        consumer.StoreOffset(result);
                        continue;
                    }

                    await _pipeline.ProcessAsync(batch, token);
                    consumer.StoreOffset(result);
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
                    await ShutdownPipelineAsync();
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

        private async Task ShutdownPipelineAsync()
        {
            if (_pipeline.HasFaulted)
                return;

            await _pipeline.CompleteInputAsync();

            // A block may have faulted while draining; the flight then stays open for the next InitAsync.
            if (_pipeline.HasFaulted)
                return;

            foreach (PersistBatch batch in _pipeline.Flush())
                await _pipeline.WriteAsync(batch);

            await _pipeline.CompleteWritesAsync();

            await _pipeline.CloseCurrentFlightAsync();
        }
    }
}