using System;
using System.Collections.Generic;
using System.Text;
using ParameterDataLib;

namespace MongoConsumer.Pipeline.Data
{
    public sealed record FlightAssignedBatch(string FlightId, string? PreviousFlightId, ParameterData[] Data);
}
