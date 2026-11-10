using System;
using System.Collections.Generic;
using System.Text;
using BaselineBuilder.ICDHandling;
using MongoLib.Documents;
using ThpUtils.Naming;

namespace MongoConsumer.DbCreation
{
    public sealed class FieldCatalogSeedProvider
    {
        private const string ContinuousKind = "continuous";
        private const string OtherKind = "other";

        private readonly IcdDeserializer _icd;
        private readonly NamingData _namingData;

        public FieldCatalogSeedProvider(IcdDeserializer icd, NamingData namingData)
        {
            _icd = icd;
            _namingData = namingData;
        }

        public IEnumerable<FieldCatalogDocument> GetSeed()
        {
            List<FieldCatalogDocument> seed = new(_icd.ParameterCount);

            foreach (IcdParameter parameter in _icd.Parameters)
            {
                string kind = OtherKind;
                Boolean usedForThp = false;

                if (_namingData.ParameterNames.Contains(parameter.Name))
                {
                    kind = ContinuousKind;
                    usedForThp = true;
                }

                seed.Add(new FieldCatalogDocument
                {
                    Field = parameter.Name,
                    Unit = parameter.Unit.ToString(),
                    Kind = kind,
                    UsedForTHP = usedForThp,
                    RateHz = parameter.Correlator,
                });
            }

            return seed;
        }
    }
}
