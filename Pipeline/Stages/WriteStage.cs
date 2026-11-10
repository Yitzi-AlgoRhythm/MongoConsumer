using System;
using System.Collections.Generic;
using System.Text;
using MongoConsumer.Pipeline.Data;
using MongoLib;

namespace MongoConsumer.Pipeline.Stages
{
    public sealed class WriteStage
    {
        private readonly ITelemetryWriter _writer;

        public WriteStage(ITelemetryWriter writer)
        {
            _writer = writer;
        }

        /// <summary>The delegate given to the ActionBlock.</summary>
        public async Task WriteAsync(PersistBatch batch)
        {
            foreach (KeyValuePair<string, List<TelemetrySample>> entry in batch.Samples)
            {
                if (entry.Value.Count == 0)
                    continue;

                await _writer.WriteSamplesAsync(batch.FlightId, entry.Key, entry.Value);
            }

            // Only reached if every write above succeeded, so SampleCounts are correct.
            if (batch.Close is not null)
                await _writer.CloseFlightAsync(batch.Close.FlightId, batch.Close.EndTime);
        }
    }
}
