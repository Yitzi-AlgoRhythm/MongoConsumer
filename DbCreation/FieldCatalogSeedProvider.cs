using System;
using System.Collections.Generic;
using System.Text;
using BaselineBuilder.ICDHandling;
using MongoLib.Documents;

namespace MongoConsumer.DbCreation
{
    public sealed class FieldCatalogSeedProvider
    {
        private const string ContinuousKind = "continuous";

        private readonly IcdDeserializer _icd;

        public FieldCatalogSeedProvider(IcdDeserializer icd)
        {
            _icd = icd;
        }

        public IEnumerable<FieldCatalogDocument> GetSeed()
        {
            List<FieldCatalogDocument> seed = new List<FieldCatalogDocument>(_icd.ParameterCount);

            foreach (IcdParameter parameter in _icd.Parameters)
            {
                seed.Add(new FieldCatalogDocument
                {
                    Field = parameter.Name,
                    Unit = parameter.Unit.ToString(),
                    Kind = ContinuousKind,
                    UsedForTHP = true,
                    RateHz = parameter.Correlator,
                });
            }

            return seed;
        }
    }
}
