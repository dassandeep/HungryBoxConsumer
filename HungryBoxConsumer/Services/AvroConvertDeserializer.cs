using Confluent.Kafka;
using SolTechnology.Avro;
using SolTechnology.Avro.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
    public class AvroConvertDeserializer<T> : IDeserializer<T>
    {
        private readonly string _schema;

        public AvroConvertDeserializer(string schema)
        {
            var confluentSchema = new ConfluentSchema(schema);
            _schema = confluentSchema.SchemaString;
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var dataArray = data.ToArray();
            var dataWithoutMagicNumber = dataArray.Skip(5);

            var result = AvroConvert.DeserializeHeadless<T>(dataWithoutMagicNumber.ToArray(), _schema);
            return result;
        }
    }
}
