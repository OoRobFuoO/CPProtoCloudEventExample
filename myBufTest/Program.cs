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

        private static Account createAccount(string id)
        {
            Account a = new Account();
            a.AccountID = id;
            a.AccountHolderID.Add("1");
            a.AccountHolderID.Add("2");
            a.AccountHolderID.Add("3");
            a.CreateDate = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(System.DateTime.Now.ToUniversalTime());

            return a;
        }

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
                                }
                                else
                                {
                                    // The 10th message (account message) should trigger this code block
                                    Console.WriteLine($"Data Type not match. Expected: {User.Descriptor}, Actual: {consumeResult.Message.Value.Data.TypeUrl}");

                                    // TODO: Is it possible to perform dynamic casting using reflection?
                                    Account a = consumeResult.Message.Value.Data.Unpack<Account>();
                                    Console.WriteLine($"Account ID: {a.AccountID}, CreateDateTime: {a.CreateDate.ToDateTime().ToLocalTime()}, Holder count: {a.AccountHolderID.Count}");
                                }
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
                    CloudEventEnevlop msg;

                    if (i==10)
                    {
                        // The 10th message will be an account message for demostration purposal
                        Account a = createAccount("123");
                        msg = createMessage(a);
                    }
                    else
                    {
                        User u = createUser($"{i}", $"User {i} First Name", $"User {i} Last Name");
                        msg = createMessage(u);
                    }
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
