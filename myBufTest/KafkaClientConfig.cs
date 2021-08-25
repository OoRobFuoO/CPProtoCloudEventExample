using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace RF.myBufTest
{
    public static class KafkaClientConfig
    {
        private static string _bootstrapServer = "";
        private static string _srURL = "";
        private static string _saslUsername = "";
        private static string _saslPassword = "";

        public static ProducerConfig pConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServer,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = _saslUsername,
            SaslPassword = _saslPassword
        };


        public static ConsumerConfig cConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServer,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = _saslUsername,
            SaslPassword = _saslPassword,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        public static SchemaRegistryConfig srConfig = new SchemaRegistryConfig
        {
            Url = _srURL,
            BasicAuthUserInfo = ""
        };
    }
}
