using System;
using Confluent.Kafka.SyncOverAsync;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System.Threading;
using System.Threading.Tasks;
using RF.MyBufTest.Schema;
using RF.MyBufTest.Schema.Data;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace RF.myBufTest
{
    class Program
    {
        private static string _topicName = "testTopic";

        private static CloudEventEnevlop createMessage(Google.Protobuf.IMessage data)
        {
            CloudEventEnevlop msg = new CloudEventEnevlop();

            msg.Specversion = "1.0";

            msg.Data = Google.Protobuf.WellKnownTypes.Any.Pack(data);

            return msg;
        }

        private static User createUser(string id, string firstName, string lastName)
        {
            User u = new User();
            u.UserID = id;
            u.Firstname = firstName;
            u.Lastname = lastName;

            return u;
        }

        /*
        private static T read<T>(Google.Protobuf.WellKnownTypes.Any data) where T : IMessage 
        {
            if (data.Is(T.Descriptor))
                return data.Unpack<T>();
            else

            return null;
        }
        */

        static async Task Main(string[] args)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var consumer =
                    new ConsumerBuilder<string, CloudEventEnevlop>(KafkaClientConfig.cConfig)
                        .SetValueDeserializer(new ProtobufDeserializer<CloudEventEnevlop>().AsSyncOverAsync())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    consumer.Subscribe(_topicName);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cts.Token);
                                if (consumeResult.Message.Value.Data.Is(User.Descriptor))
                                {
                                    User u = consumeResult.Message.Value.Data.Unpack<User>();
                                    
                                    Console.WriteLine($"User ID: {u.UserID}, FirstName: {u.Firstname}, LastName: {u.Lastname}");
                                    Console.WriteLine($"Type URL {consumeResult.Message.Value.Data.TypeUrl}");
                                }
                                else
                                    Console.WriteLine($"Data Type not match. Expected: {User.Descriptor}, Actual: {consumeResult.Message.Value.Data.TypeUrl}");
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Consume error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });

            using (var schemaRegistry = new CachedSchemaRegistryClient(KafkaClientConfig.srConfig))   
            using (var producer =
                    new ProducerBuilder<string, CloudEventEnevlop>(KafkaClientConfig.pConfig)
                    .SetValueSerializer(new ProtobufSerializer<CloudEventEnevlop>(schemaRegistry))
                    .Build())
            {
                int i = 0;
                while(true)
                {
                    i++;
                    User u = createUser($"{i}", $"User {i} First Name", $"User {i} Last Name");
                    CloudEventEnevlop msg = createMessage(u);

                    await producer
                       .ProduceAsync(_topicName, new Message<string, CloudEventEnevlop> { Key = i.ToString(), Value = msg })
                       .ContinueWith(task => task.IsFaulted
                           ? $"error producing message: {task.Exception.Message}"
                           : $"produced to: {task.Result.TopicPartitionOffset}");

                    System.Threading.Thread.Sleep(5000);
                }
                
            }

        }
    }
}
