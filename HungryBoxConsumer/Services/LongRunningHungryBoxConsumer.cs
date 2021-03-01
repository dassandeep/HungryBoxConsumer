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
 }
