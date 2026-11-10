using System;
using System.Collections.Generic;
using System.Text;
using MongoConsumer.Pipeline.Data;
using MongoLib;
using MongoLib.Documents;
using MongoLib.Helpers;
using ParameterDataLib;

namespace MongoConsumer.Pipeline.Stages
{
    public sealed class CollectStage
    {
        private readonly ITelemetryReader _reader;
        private readonly Dictionary<string, FieldState> _fields = [];

        private HashSet<string> _thpFields = [];
        private string? _flightId;
        private DateTime? _latestTimestamp;

        public CollectStage(ITelemetryReader reader)
        {
            _reader = reader;
        }

        /// <summary>
        /// Loads the UsedForTHP field names from the catalog.
        /// Must run after the seeding has finished.
        /// </summary>
        public async Task InitAsync(CancellationToken ct = default)
        {
            IReadOnlyList<FieldCatalogDocument> catalog = await _reader.GetFieldCatalogAsync(ct);

            _thpFields = catalog
                .Where(f => f.UsedForTHP)
                .Select(f => f.Field)
                .ToHashSet();

            if (_thpFields.Count == 0)
                throw new InvalidOperationException("The field catalog contains no UsedForTHP fields.");
        }

        /// <summary>The delegate given to the TransformManyBlock.</summary>
        public IEnumerable<PersistBatch> Collect(FlightAssignedBatch batch)
        {
            List<PersistBatch> output = [];

            // Flight change: emit everything still buffered for the old flight, together with its close request.
            if (batch.PreviousFlightId is not null)
            {
                DateTime endTime = _latestTimestamp ?? throw new InvalidDataException("No timestamp found for previous flight.");

                output.Add(new PersistBatch(
                    batch.PreviousFlightId,
                    DrainAll(),
                    new CloseRequest(batch.PreviousFlightId, endTime)));

                _fields.Clear();
                _latestTimestamp = null;
            }

            _flightId = batch.FlightId;

            // Completed field-slabs of this call. A field never appears twice in one PersistBatch,
            // so each (flight, field, slab) is written exactly once.
            Dictionary<string, List<TelemetrySample>> pending = [];

            void AddCompleted(string field, List<TelemetrySample> samples)
            {
                if (pending.ContainsKey(field))
                {
                    output.Add(new PersistBatch(batch.FlightId, pending, null));
                    pending = [];
                }

                pending[field] = samples;
            }

            foreach (ParameterData data in batch.Data)
            {
                DateTime timestamp = ToUtc(data.TimestampMS);

                if (_latestTimestamp is null || timestamp > _latestTimestamp.Value)
                    _latestTimestamp = timestamp;
            }

            IEnumerable<IGrouping<string, ParameterData>> groups = batch.Data
                .Where(d => _thpFields.Contains(d.Name))
                .GroupBy(d => d.Name);

            foreach (IGrouping<string, ParameterData> group in groups)
            {
                string field = group.Key;

                if (!_fields.TryGetValue(field, out FieldState? state))
                {
                    state = new FieldState();
                    _fields[field] = state;
                }

                foreach (ParameterData sample in group.OrderBy(d => d.TimestampMS))
                {
                    Console.WriteLine("Collecting sample " + sample.Name);

                    DateTime timestamp = ToUtc(sample.TimestampMS);
                    long micros = SlabMath.MicrosSinceEpoch(timestamp);

                    if (micros < 0)
                        continue;

                    // Late or duplicate sample: drop it, so timestamps stay strictly increasing per field.
                    if (state.LastAccepted.HasValue && timestamp <= state.LastAccepted.Value)
                        continue;

                    int slab = SlabMath.SlabOf(micros);

                    // The field moved into a new slab: its buffer is complete.
                    if (state.Samples.Count > 0 && slab > state.Slab)
                    {
                        AddCompleted(field, state.Samples);
                        state.Samples = new List<TelemetrySample>();
                    }

                    state.Slab = slab;
                    state.Samples.Add(new TelemetrySample(timestamp, sample.Value));
                    state.LastAccepted = timestamp;
                }
            }

            if (pending.Count > 0)
                output.Add(new PersistBatch(batch.FlightId, pending, null));

            return output;
        }

        /// <summary>
        /// Shutdown step. Returns the remaining partial slabs of the current flight (Close = null,
        /// because CloseCurrentFlightAsync closes it afterwards). Call only after this block has completed.
        /// </summary>
        public IReadOnlyList<PersistBatch> Flush()
        {
            Dictionary<string, List<TelemetrySample>> remaining = DrainAll();

            if (_flightId is null || remaining.Count == 0)
                return [];

            return [new PersistBatch(_flightId, remaining, null)];
        }

        private Dictionary<string, List<TelemetrySample>> DrainAll()
        {
            Dictionary<string, List<TelemetrySample>> result = [];

            foreach (KeyValuePair<string, FieldState> entry in _fields)
            {
                if (entry.Value.Samples.Count == 0)
                    continue;

                result[entry.Key] = entry.Value.Samples;
                entry.Value.Samples = new List<TelemetrySample>();
            }

            return result;
        }

        private static DateTime ToUtc(ulong timestampMs)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((long)timestampMs).UtcDateTime;
        }

        private sealed class FieldState
        {
            public List<TelemetrySample> Samples { get; set; } = [];

            public int Slab { get; set; }

            public DateTime? LastAccepted { get; set; }
        }
    }
}
