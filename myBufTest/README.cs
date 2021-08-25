# MyBufTest

## Requirements

* https://github.com/bufbuild/buf
* Official Protobuf C# library
* https://github.com/confluentinc/confluent-kafka-dotnet version 1.7


## Step to prepare

. Run `buf generate Schema/proto` to generate code-binding
. Update the Kafka cluster connectivity with the correct setting in `KafkaClientConfig.cs`
. Change the _topicName to the correct test topic name in `Program.cs`
. Build and run the solution


# Note

This demo will push a incremented User message every 5 seconds with the 10th message (at 50 second after execution), it will push an account message.