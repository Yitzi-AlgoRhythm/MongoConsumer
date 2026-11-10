using System;
using System.Collections.Generic;
using System.Text;

namespace MongoConsumer.Pipeline.Data
{
    public sealed record CloseRequest(string FlightId, DateTime EndTime);
}
