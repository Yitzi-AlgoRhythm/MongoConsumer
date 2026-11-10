using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Hosting;
using MongoConsumer.Pipeline.Data;
using MongoConsumer.Pipeline.Stages;
using ParameterDataLib;

namespace MongoConsumer.Pipeline
{
    public sealed class TelemetryPipeline
    {
        private const int BlockCapacity = 1000;

        private readonly FlightAssignmentStage _flightStage;
        private readonly CollectStage _collectStage;
        private readonly IHostApplicationLifetime _lifetime;

        private readonly TransformBlock<ParameterData[], FlightAssignedBatch> _assignBlock;
        private readonly TransformManyBlock<FlightAssignedBatch, PersistBatch> _collectBlock;
        private readonly ActionBlock<PersistBatch> _writeBlock;

        public TelemetryPipeline
        (
            FlightAssignmentStage flightStage,
            CollectStage collectStage,
            WriteStage writeStage,
            IHostApplicationLifetime lifetime
        )
        {
            _flightStage = flightStage;
            _collectStage = collectStage;
            _lifetime = lifetime;

            _assignBlock = new TransformBlock<ParameterData[], FlightAssignedBatch>(
                flightStage.AssignFlightAsync,
                CreateOptions());

            _collectBlock = new TransformManyBlock<FlightAssignedBatch, PersistBatch>(
                collectStage.Collect,
                CreateOptions());

            _writeBlock = new ActionBlock<PersistBatch>(
                writeStage.WriteAsync,
                CreateOptions());

            // Stage 1 -> 2 propagates completion and faults.
            _assignBlock.LinkTo(_collectBlock, new DataflowLinkOptions { PropagateCompletion = true });

            // Stage 2 -> 3 does not propagate completion, so the shutdown flush items can still be posted.
            _collectBlock.LinkTo(_writeBlock);

            // Faults must still reach stage 3 manually.
            _collectBlock.Completion.ContinueWith(
                t =>
                {
                    if (t.IsFaulted)
                        ((IDataflowBlock)_writeBlock).Fault(t.Exception!);
                },
                TaskScheduler.Default);

            // A fault anywhere ends up here, and the consumer's SendAsync would otherwise wait forever.
            _writeBlock.Completion.ContinueWith(
                t =>
                {
                    if (t.IsFaulted)
                        _lifetime.StopApplication();
                },
                TaskScheduler.Default);
        }

        /// <summary>True if any block has faulted. When true, skip the flush and the close on shutdown.</summary>
        public bool HasFaulted =>
            _assignBlock.Completion.IsFaulted
            || _collectBlock.Completion.IsFaulted
            || _writeBlock.Completion.IsFaulted;

        // ----- Startup -----

        public Task InitFlightAssignmentAsync(CancellationToken ct = default)
        {
            return _flightStage.InitAsync(ct);
        }

        /// <summary>Loads the UsedForTHP names from the catalog. Must run after the seeding.</summary>
        public Task InitCollectAsync(CancellationToken ct = default)
        {
            return _collectStage.InitAsync(ct);
        }

        // ----- Runtime (called by the Kafka consumer) -----

        public async Task ProcessAsync(ParameterData[] data, CancellationToken ct = default)
        {
            bool accepted = await _assignBlock.SendAsync(data, ct);

            if (!accepted)
                throw new InvalidOperationException("The pipeline is no longer accepting data.");
        }

        // ----- Shutdown (called by the lifecycle service, in this order) -----

        /// <summary>Completes stage 1 and waits until stage 2 has drained its input.</summary>
        public async Task CompleteInputAsync()
        {
            _assignBlock.Complete();
            await _collectBlock.Completion;
        }

        /// <summary>Returns the remaining partial slabs. Call only after CompleteInputAsync.</summary>
        public IReadOnlyList<PersistBatch> Flush()
        {
            return _collectStage.Flush();
        }

        /// <summary>Queues an item on stage 3, behind everything already waiting there.</summary>
        public async Task WriteAsync(PersistBatch batch)
        {
            bool accepted = await _writeBlock.SendAsync(batch);

            if (!accepted)
                throw new InvalidOperationException("The write stage is no longer accepting items.");
        }

        /// <summary>Completes stage 3 and waits until every queued write has finished.</summary>
        public async Task CompleteWritesAsync()
        {
            _writeBlock.Complete();
            await _writeBlock.Completion;
        }

        public Task CloseCurrentFlightAsync(CancellationToken ct = default)
        {
            return _flightStage.CloseCurrentFlightAsync(ct);
        }

        private static ExecutionDataflowBlockOptions CreateOptions()
        {
            return new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = BlockCapacity
            };
        }
    }
}
