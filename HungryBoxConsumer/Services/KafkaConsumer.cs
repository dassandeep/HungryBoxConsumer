using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
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
}
