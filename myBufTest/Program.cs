using System;
using Confluent.Kafka.SyncOverAsync;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System.Threading;
using System.Threading.Tasks;
using RF.MyBufTest.Schema;
using RF.MyBufTest.Schema.Data;
using myJsonSchema = RF.MyBufTest.Schema.JsonSchema;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace RF.myBufTest
{
    class CEBuilder
    {
        private static TypeRegistry _registry = TypeRegistry.FromMessages(CloudEvent.Descriptor, User.Descriptor, Account.Descriptor);

        private static CloudEvent createProtoMessage(Google.Protobuf.IMessage data, bool Json)
        {
            CloudEvent msg = new CloudEvent();

            msg.SpecVersion = "1.0";
            msg.Type = data.Descriptor.FullName;
            msg.Id = new Guid().ToString();

            if (Json)
                msg.Datacontenttype = "JSON";
            else
                msg.Datacontenttype = "Protobuf";

            msg.Time = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.Now.ToUniversalTime());

            msg.Data = Google.Protobuf.WellKnownTypes.Any.Pack(data);

            return msg;
        }

        public static CloudEvent createProtoMessage(Google.Protobuf.IMessage data)
        {
            return createProtoMessage(data, false);
        }

        public static CloudEvent parseFromJson(string json)
        {
            JsonParser parser = new JsonParser(new JsonParser.Settings(10, _registry));

            return parser.Parse<CloudEvent>(json);
        }

        public static string createJsonMessageFromProto(Google.Protobuf.IMessage data)
        {
            CloudEvent ce = createProtoMessage(data, true);

            var registry = TypeRegistry.FromMessages(CloudEvent.Descriptor, data.Descriptor);

            var f = new Google.Protobuf.JsonFormatter(new JsonFormatter.Settings(true, registry));

            return f.Format(ce);
        }

        public static string createJsonMessage(object messageObj)
        {
            myJsonSchema.CloudEvent ce = new myJsonSchema.CloudEvent
            {
                specVersion = "1.0",
                type = messageObj.GetType().FullName.ToString(),
                id = new Guid().ToString(),
                datacontenttype = "JSON",
                time = DateTime.Now.ToLocalTime(),
                data = messageObj
            };

            return JsonConvert.SerializeObject(ce);
        }
    }

    class Program
    {
        private static string _topicName_proto = "protoTest";
        private static string _topicName_json = "jsonTest";
        private static string _topicName_jsonToproto = "jsonToprotoTest";
        private static List<Task> _tasks = new List<Task>();
        private static CancellationTokenSource _cts = new CancellationTokenSource();
        private static int _producerWaitTime = 250;

        private static User createUser(string id, string firstName, string lastName)
        {
            User u = new User();
            u.UserID = id;
            u.Firstname = firstName;
            u.Lastname = lastName;

            return u;
        }

        private static myJsonSchema.User createUserJson(string id, string firstName, string lastName)
        {
            myJsonSchema.User u = new myJsonSchema.User
            {
                userID = id,
                firstname = firstName,
                lastname = lastName
            };

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
     
        static void Main(string[] args)
        {
            ProtoMode();
            JsonMode();
            JsonToProtoMode();

            Task.WaitAll(_tasks.ToArray());
        }

        static void ProtoMode()
        {
            string _mode = "Protobuf";

            var consumeTask = Task.Run(() =>
            {
                using (var consumer =
                    new ConsumerBuilder<string, CloudEvent>(KafkaClientConfig.cConfig)
                        .SetValueDeserializer(new ProtobufDeserializer<CloudEvent>().AsSyncOverAsync())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    consumer.Subscribe(_topicName_proto);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(_cts.Token);
                                if (consumeResult != null)
                                {
                                    Console.WriteLine($"mode - [{_mode}] / Consumed message key: {consumeResult.Message.Key}, message value {consumeResult.Message.Value}");
                                    if (consumeResult.Message.Value.Data.Is(User.Descriptor))
                                    {
                                        User u = consumeResult.Message.Value.Data.Unpack<User>();

                                        Console.WriteLine($"mode - [{_mode}] / User ID: {u.UserID}, FirstName: {u.Firstname}, LastName: {u.Lastname}");
                                    }
                                    else
                                    {
                                        // The 10th message (account message) should trigger this code block
                                        Console.WriteLine($"mode - [{_mode}] / Data Type not match. Expected: {User.Descriptor.FullName}, Actual: {consumeResult.Message.Value.Data.TypeUrl}");

                                        // TODO: Is it possible to perform dynamic casting using reflection?
                                        Account a = consumeResult.Message.Value.Data.Unpack<Account>();
                                        Console.WriteLine($"mode - [{_mode}] / Account ID: {a.AccountID}, CreateDateTime: {a.CreateDate.ToDateTime().ToLocalTime()}, Holder count: {a.AccountHolderID.Count}");
                                    }
                                }
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"mode - [{_mode}] / Consume error: {e.Error.Reason}");
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"mode - [{_mode}] / Consume error: {e.Message}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });


            var producerTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(KafkaClientConfig.srConfig))
                using (var producer =
                        new ProducerBuilder<string, CloudEvent>(KafkaClientConfig.pConfig_Proto)
                        .SetValueSerializer(new ProtobufSerializer<CloudEvent>(schemaRegistry))
                        .Build())
                {
                    int i = 0;
                    while (true)
                    {
                        i++;
                        CloudEvent msg;

                        if (i == 10)
                        {
                            // The 10th message will be an account message for demostration purposal
                            Account a = createAccount("123");
                            msg = CEBuilder.createProtoMessage(a);
                        }
                        else
                        {
                            User u = createUser($"{i}", $"User {i} First Name", $"User {i} Last Name");
                            msg = CEBuilder.createProtoMessage(u);
                        }

                        producer
                           .ProduceAsync(_topicName_proto, new Message<string, CloudEvent> { Key = i.ToString(), Value = msg })
                           .ContinueWith(task => task.IsFaulted
                               ? $"mode - [{_mode}] / error producing message: {task.Exception.Message}"
                               : $"mode - [{_mode}] / produced to: {task.Result.TopicPartitionOffset}");

                        producer.Flush(TimeSpan.FromSeconds(10));
                        Console.WriteLine($"mode - [{_mode}] / Produced {i}");
                        System.Threading.Thread.Sleep(_producerWaitTime);
                    }
                }
            });

            _tasks.Add(consumeTask);
            _tasks.Add(producerTask);
        }

        static void JsonMode()
        {
            string _mode = "Json";

            var consumeTask = Task.Run(() =>
            {
                using (var consumer =
                    new ConsumerBuilder<string, string>(KafkaClientConfig.cConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    consumer.Subscribe(_topicName_json);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(_cts.Token);
                                
                                if (consumeResult.Message != null)
                                {
                                    Console.WriteLine($"mode - [{_mode}] / Message: {consumeResult.Message.Value}");

                                    CloudEvent ce = CEBuilder.parseFromJson(consumeResult.Message.Value);
                                    if (ce.Data.TypeUrl.Contains("User"))
                                    {
                                        User u = ce.Data.Unpack<User>();
                                        Console.WriteLine($"mode - [{_mode}] / User ID: {u.UserID}, FirstName: {u.Firstname}, LastName: {u.Lastname}");
                                    }
                                    else
                                    {
                                        Account a = ce.Data.Unpack<Account>();
                                        Console.WriteLine($"mode - [{_mode}] / Account ID: {a.AccountID}, CreateDateTime: {a.CreateDate.ToDateTime().ToLocalTime()}, Holder count: {a.AccountHolderID.Count}");
                                    }
                                }
                                else
                                    Console.WriteLine($"mode - [{_mode}] / No message");
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"mode - [{_mode}] / Consume error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });

            var producerTask = Task.Run(() =>
            {
                using (var producer =
                    new ProducerBuilder<string, string>(KafkaClientConfig.pConfig_Json_SnappyCompressed).Build())
                {
                    int i = 1000;
                    while (true)
                    {
                        i++;
                        string msg;

                        if (i == 1010)
                        {
                            // The 10th message will be an account message for demostration purposal
                            Account a = createAccount("123");
                            msg = CEBuilder.createJsonMessageFromProto(a);
                        }
                        else
                        {
                            User u = createUser($"{i}", $"User {i} First Name", $"User {i} Last Name");
                            msg = CEBuilder.createJsonMessageFromProto(u);
                        }

                        producer
                           .ProduceAsync(_topicName_json, new Message<string, string> { Key = i.ToString(), Value = msg })
                           .ContinueWith(task => task.IsFaulted
                               ? $"mode - [{_mode}] / error producing message: {task.Exception.Message}"
                               : $"mode - [{_mode}] / produced to: {task.Result.TopicPartitionOffset}");

                        producer.Flush(TimeSpan.FromSeconds(10));
                        Console.WriteLine($"mode - [{_mode}] / Produced {i}");

                        System.Threading.Thread.Sleep(_producerWaitTime);
                    }

                }
            });

            _tasks.Add(consumeTask);
            _tasks.Add(producerTask);
        }

        static void JsonToProtoMode()
        {
            string _mode = "JsonSchema";

            var consumeTask = Task.Run(() =>
            {
                using (var consumer =
                    new ConsumerBuilder<string, string>(KafkaClientConfig.cConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    consumer.Subscribe(_topicName_jsonToproto);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(_cts.Token);
                                if (consumeResult.Message != null)
                                {
                                    Console.WriteLine($"mode - [{_mode}] / Message: {consumeResult.Message.Value}");

                                    CloudEvent ce = CEBuilder.parseFromJson(consumeResult.Message.Value);
                                    try
                                    {
                                        User u = ce.Data.Unpack<User>();
                                        Console.WriteLine($"mode - [{_mode}] / User ID: {u.UserID}, FirstName: {u.Firstname}, LastName: {u.Lastname}");
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine($"mode - [{_mode}] / Failed to parser message: {e} \n StackTrace: {e.StackTrace}");
                                    }                                    
                                }
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"mode - [{_mode}] / Consume error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });

            var producerTask = Task.Run(() =>
            {
                using (var producer =
                    new ProducerBuilder<string, string>(KafkaClientConfig.pConfig_Json_SnappyCompressed).Build())
                {
                    int i = 100000;
                    while (true)
                    {
                        i++;
                                                
                        myJsonSchema.User u = createUserJson($"{i}", $"User {i} First Name", $"User {i} Last Name");
                        string msg = CEBuilder.createJsonMessage(u);
                        
                        producer
                           .ProduceAsync(_topicName_jsonToproto, new Message<string, string> { Key = i.ToString(), Value = msg })
                           .ContinueWith(task => task.IsFaulted
                               ? $"mode - [{_mode}] / error producing message: {task.Exception.Message}"
                               : $"mode - [{_mode}] / produced to: {task.Result.TopicPartitionOffset}");

                            producer.Flush(TimeSpan.FromSeconds(10));
                        Console.WriteLine($"mode - [{_mode}] / Produced {i}");

                        System.Threading.Thread.Sleep(_producerWaitTime);
                    }

                }
            });

            _tasks.Add(consumeTask);
            _tasks.Add(producerTask);
        }
    }
}
