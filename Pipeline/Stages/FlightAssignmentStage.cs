using System;
using System.Collections.Generic;
using System.Text;
using MongoConsumer.Pipeline.Data;
using MongoLib;
using MongoLib.Documents;
using ParameterDataLib;

namespace MongoConsumer.Pipeline.Stages
{
    public sealed class FlightAssignmentStage
    {
        private static readonly TimeSpan MaxGap = TimeSpan.FromSeconds(10);

        private readonly ITelemetryReader _reader;
        private readonly ITelemetryWriter _writer;

        private string? _currentFlightId;
        private DateTime? _latestTimestamp;

        public FlightAssignmentStage(ITelemetryReader reader, ITelemetryWriter writer)
        {
            _reader = reader;
            _writer = writer;
        }

        /// <summary>
        /// Startup step: closes a leftover open flight (end time = its last stored sample,
        /// or its start time if it has none) and creates a new one.
        /// Must complete before the pipeline receives any data.
        /// </summary>
        public async Task InitAsync(CancellationToken ct = default)
        {
            FlightDocument? openFlight = await _reader.GetOpenFlightAsync(ct);

            if (openFlight is not null)
            {
                DateTime? lastSampleTime = await _reader.GetLastSampleTimeAsync(openFlight.Id, ct);
                await _writer.CloseFlightAsync(openFlight.Id, lastSampleTime ?? openFlight.StartTime, ct);
            }

            FlightDocument newFlight = await _writer.CreateFlightAsync(DateTime.UtcNow, ct);

            _currentFlightId = newFlight.Id;
            _latestTimestamp = null;
        }

        /// <summary>The delegate given to the TransformBlock.</summary>
        public async Task<FlightAssignedBatch> AssignFlightAsync(ParameterData[] data)
        {
            if (_currentFlightId is null)
                throw new InvalidOperationException($"{nameof(InitAsync)} must be called before processing data.");

            if (data.Length == 0)
                return new FlightAssignedBatch(_currentFlightId, null, data);

            ulong minMs = ulong.MaxValue;
            ulong maxMs = ulong.MinValue;

            foreach (ParameterData parameter in data)
            {
                if (parameter.TimestampMS < minMs)
                    minMs = parameter.TimestampMS;
                if (parameter.TimestampMS > maxMs)
                    maxMs = parameter.TimestampMS;
            }

            DateTime earliest = DateTimeOffset.FromUnixTimeMilliseconds((long)minMs).UtcDateTime;
            DateTime latest = DateTimeOffset.FromUnixTimeMilliseconds((long)maxMs).UtcDateTime;

            string? previousFlightId = null;

            if (_latestTimestamp.HasValue && (earliest - _latestTimestamp.Value).Duration() > MaxGap)
            {
                FlightDocument newFlight = await _writer.CreateFlightAsync(earliest);

                previousFlightId = _currentFlightId;
                _currentFlightId = newFlight.Id;
                _latestTimestamp = latest;

                Console.WriteLine("Created new flight " + _currentFlightId);
            }
            else if (!_latestTimestamp.HasValue || latest > _latestTimestamp.Value)
            {
                _latestTimestamp = latest;
            }

            return new FlightAssignedBatch(_currentFlightId, previousFlightId, data);
        }

        /// <summary>
        /// Shutdown step. Call only after the pipeline has completed (stage 3 finished all writes).
        /// Closes the current flight using the latest data timestamp seen, or its start time if no data arrived.
        /// </summary>
        public async Task CloseCurrentFlightAsync(CancellationToken ct = default)
        {
            if (_currentFlightId is null)
                return;

            DateTime endTime;

            if (_latestTimestamp.HasValue)
            {
                endTime = _latestTimestamp.Value;
            }
            else
            {
                FlightDocument? flight = await _reader.GetFlightAsync(_currentFlightId, ct);
                endTime = flight?.StartTime ?? DateTime.UtcNow;
            }

            await _writer.CloseFlightAsync(_currentFlightId, endTime, ct);
            _currentFlightId = null;
        }
    }
}
