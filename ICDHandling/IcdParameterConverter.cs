using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using ParameterDataLib.Enums;

namespace BaselineBuilder.ICDHandling
{
    public class IcdParameterConverter : JsonConverter<IcdParameter>
    {
        public override IcdParameter Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            using JsonDocument doc = JsonDocument.ParseValue(ref reader);
            JsonElement root = doc.RootElement;

            string name = root.GetProperty("Name").GetString()!;

            string description = root.GetProperty("Description").GetString()!;

            ParameterDataType type = JsonSerializer.Deserialize<ParameterDataType>(root.GetProperty("DataType").GetRawText(), options);

            int offset = root.GetProperty("Offset").GetInt32();

            int size = root.GetProperty("Size").GetInt32();

            int correlator = root.GetProperty("Correlator").GetInt32();

            TelemetryUnit unit = JsonSerializer.Deserialize<TelemetryUnit>(root.GetProperty("Units").GetRawText(), options);

            JsonElement range = root.GetProperty("Range");

            double min = range.GetProperty("Min").GetDouble();
            double max = range.GetProperty("Max").GetDouble();

            string bitMaskRaw = root.GetProperty("Mask").GetString()!;

            byte[] bitMask = Convert.FromHexString(bitMaskRaw[2..]);

            return new IcdParameter
            {
                Name = name,
                Description = description,
                Type = type,
                Offset = offset,
                Size = size,
                BitMask = bitMask,
                Correlator = correlator,
                Unit = unit,
                Min = min,
                Max = max
            };
        }

        public override void Write(
            Utf8JsonWriter writer,
            IcdParameter value,
            JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }
}
