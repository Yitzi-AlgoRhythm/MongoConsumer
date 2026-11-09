using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace BaselineBuilder.ICDHandling
{
    public class IcdDeserializer
    {
        public required Dictionary<byte, List<IcdParameter>> CorrelatorGroups { get; init; }

        public required List<IcdParameter> Parameters { get; init; }

        public required int ParameterCount { get; init; }


        public IcdDeserializer()
        {
            List<IcdParameter> icdParameters;

            using (FileStream stream = File.OpenRead(IcdConstants.IcdFile))
            {
                icdParameters = JsonSerializer.Deserialize<List<IcdParameter>>(stream)!;
            }

            Parameters = icdParameters;

            ParameterCount = icdParameters.Count;

            CorrelatorGroups = [];

            foreach (IcdParameter parameter in icdParameters)
            {
                byte paramCorrelator = (byte)parameter.Correlator;

                if (!CorrelatorGroups.ContainsKey(paramCorrelator))
                {
                    CorrelatorGroups.Add(paramCorrelator, []);
                }

                CorrelatorGroups[paramCorrelator].Add(parameter);
            }
        }
    }
}
