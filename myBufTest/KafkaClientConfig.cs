using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace RF.myBufTest
{
    public static class KafkaClientConfig
    {
        private static string _bootstrapServer = "";
        private static string _saslUsername = "";
        private static string _saslPassword = "";

        private static string _srURL = "";
        private static string _srUserInfo = "";

        public static ProducerConfig pConfig_Proto = new ProducerConfig
        {
            BootstrapServers = _bootstrapServer,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = _saslUsername,
            SaslPassword = _saslPassword,
            Acks = Acks.All
        };

        public static ProducerConfig pConfig_Json_SnappyCompressed = new ProducerConfig
        {
            BootstrapServers = _bootstrapServer,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = _saslUsername,
            SaslPassword = _saslPassword,
            Acks = Acks.All,
            CompressionType = CompressionType.Snappy
        };

        public static ProducerConfig pConfig_Json_ZstdCompressed = new ProducerConfig
        {
            BootstrapServers = _bootstrapServer,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = _saslUsername,
            SaslPassword = _saslPassword,
            Acks = Acks.All,
            CompressionType = CompressionType.Zstd
        };

        public static ConsumerConfig cConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServer,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = _saslUsername,
            SaslPassword = _saslPassword,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Latest,
            SessionTimeoutMs = 6000
        };

        public static SchemaRegistryConfig srConfig = new SchemaRegistryConfig
        {
            Url = _srURL,
            BasicAuthUserInfo = _srUserInfo
        };
    }
}
