using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Producer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using var p = new ProducerBuilder<Null, string>(config).Build();
            {
                try
                {
                    var count = 0;
                    while (true)
                    {
                        var dr = await p.ProduceAsync("test-topic",
                            new Message<Null, string>
                            {
                                Value = $"This is the message number {count}"
                            });

                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}' | {count}");
                        count++;
                        Thread.Sleep(2000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery Failed: { e.Error.Reason }");
                }
            }
        }
    }
}
