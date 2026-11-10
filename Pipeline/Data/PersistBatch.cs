using MongoLib;

namespace MongoConsumer.Pipeline.Data
{
    public sealed record PersistBatch(
        string FlightId,
        Dictionary<string, List<TelemetrySample>> Samples,
        CloseRequest? Close);
}
