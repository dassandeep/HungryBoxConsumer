using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
    public static class ConsumerBuilderExtensions
    {
        public static ConsumerBuilder<TKey, TValue> SetAvroValueDeserializer<TKey, TValue>(this ConsumerBuilder<TKey, TValue> consumerBuilder, string schema)
        {
            var des = new AvroConvertDeserializer<TValue>(schema);
            return consumerBuilder.SetValueDeserializer(des);
        }
    }
}
