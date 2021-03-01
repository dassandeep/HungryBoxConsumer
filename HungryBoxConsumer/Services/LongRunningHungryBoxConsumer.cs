using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SolTechnology.Avro;
using SolTechnology.Avro.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
    public class LongRunningHungryBoxConsumer : BackgroundService
    {
        private readonly ILogger<LongRunningHungryBoxConsumer> logger;
        private IAppSettings _configuration;
        public LongRunningHungryBoxConsumer(ILogger<LongRunningHungryBoxConsumer> logger)
        {
            this.logger = logger;
        }
        public LongRunningHungryBoxConsumer(IAppSettings appSettings) => this._configuration = appSettings;
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            SchemaFile.GetSchemaAvro(out string schema);
            Dictionary<string, object> configValuePairs = _configuration.GetConfigValue();
            return Task.Run(() =>
            {
               SubscribeAsync(configValuePairs, schema);
            });
        }
        public void SubscribeAsync(Dictionary<string, object> config, string schema)
        {
            try
            {

                var consumer = new ConsumerBuilder<string, Food>(new ConsumerConfig
                {
                    BootstrapServers = (string)config[KafkaPropNames.BootstrapServers],
                    GroupId = (string)config[KafkaPropNames.GroupId],
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    EnablePartitionEof = true,
                    StatisticsIntervalMs = 5000,
                    SessionTimeoutMs = 6000,
                }).SetKeyDeserializer(Deserializers.Utf8)
                .SetAvroValueDeserializer(schema)
                .Build();
                var topic = (string)config[KafkaPropNames.Topic];
                consumer.Assign(new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset(topic, (int)config[KafkaPropNames.Partition], (int)config[KafkaPropNames.Offset]),

            });

                var handler = new Handler();
                var kafkaConsumer = new KafkaConsumer<string, Food>(
                       consumer,
                      (key, value, utcTimestamp) =>
                      {
                          //Console.WriteLine($"C#     {key}  ->  ");
                          //Console.WriteLine($"   {utcTimestamp}");
                          handler.Handle(value);
                          //Console.WriteLine(value);
                      }, CancellationToken.None)
                  .StartConsuming();
            }
            catch (ConsumeException ex)
            {
                ex.ToString();
            }

            catch (OperationCanceledException ex)
            {
                ex.ToString();
            }
        }
    }
    public static class ConsumerBuilderExtensions
    {
        public static ConsumerBuilder<TKey, TValue> SetAvroValueDeserializer<TKey, TValue>(this ConsumerBuilder<TKey, TValue> consumerBuilder, string schema)
        {
            var des = new AvroConvertDeserializer<TValue>(schema);
            return consumerBuilder.SetValueDeserializer(des);
        }
    }
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
    public class KafkaConsumer<TKey, TValue> : IDisposable
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly Action<TKey, TValue, DateTime> _handler;
        private readonly CancellationToken _cancellationToken;
        private Task _taskConsumer;


        public KafkaConsumer(IConsumer<TKey, TValue> consumer, Action<TKey, TValue, DateTime> handler, CancellationToken cancellationToken)
        {
            _consumer = consumer;
            _handler = handler;
            _cancellationToken = cancellationToken;
        }


        public KafkaConsumer<TKey, TValue> StartConsuming()
        {
            _taskConsumer = StartConsumingInner();
            return this;
        }


        private async Task StartConsumingInner()
        {
            try
            {

                while (!_cancellationToken.IsCancellationRequested)
                {
                    //ConsumeResult<TKey, TValue> consumeResult = await Task.Run(() => _consumer.Consume(_cancellationToken));
                    //if (consumeResult.Message!=null)
                    //{
                    //    _handler(consumeResult.Message.Key, consumeResult.Message.Value, consumeResult.Message.Timestamp.UtcDateTime);
                    //}
                    ConsumeResult<TKey, TValue> consumeResult = await Task.Run(() => _consumer.Consume(_cancellationToken));

                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine(
                            $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                        continue;
                    }

                    else
                    {
                        if (consumeResult.Message != null)
                        {
                            _handler(consumeResult.Message.Key, consumeResult.Message.Value, consumeResult.Message.Timestamp.UtcDateTime);
                            //_consumer.Commit(consumeResult);
                        }
                    }
                    //if (consumeResult.Offset % 0 == 0)
                    //{
                    //    try
                    //    {
                    //        _consumer.Commit(consumeResult);
                    //    }
                    //    catch (KafkaException e)
                    //    {
                    //        Console.WriteLine($"Commit error: {e.Error.Reason}");
                    //    }
                    //}
                }
            }
            catch (OperationCanceledException)
            {

                _consumer.Close();
            }

        }


        public void Dispose()
        {
            _taskConsumer?.Wait(_cancellationToken);
        }
    }
    public class Handler
    {
        public void Handle(Food recordModel)
        {
            Console.WriteLine(JsonConvert.SerializeObject(recordModel));

        }

    }
}
