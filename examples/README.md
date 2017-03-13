## Building the example
mvn clean package 

## Running the example
java -jar target/cherami-client-java-example.jar --help
ParseError: Not enough arguments
Usage: java com.uber.cherami.example.Demo
    --endpoint=[frontEndIP:Port]   Cherami server ip address and port number
    --nMsgsToSend=[nMsgsToSend]    Total number of messages to publish
    --msgSize=[msgSize]            Size of each published message
    --nPublishers=[nPublishers]    Number of publisher threads, optional
    --nConsumers=[nConsumers]      Number of consumer threads, optional
    --useAsync=[true]              Use async api, defaults to sync api
    --help                         Prints this message

## Example Run
java -jar cherami-client-java-example.jar --endpoint=127.0.0.1:4922 --nMsgsToSend=10000 --msgSize=1024 --useAsync=true

Created Destination:
DestinationDescription(path:/test/java.example_1872280763, type:PLAIN, status:ENABLED, consumedMessagesRetention:3600, unconsumedMessagesRetention:7200, destinationUUID:8f1514e0-5372-4d78-8142-a2b98e0542a5, ownerEmail:cherami-client-example@uber.com, checksumOption:CRC32IEEE, isMultiZone:false)
Created Consumer Group:
ConsumerGroupDescription(destinationPath:/test/java.example_1872280763, consumerGroupName:/test/java.example_1872280763_reader, startFrom:9932924662839619, status:ENABLED, lockTimeoutInSeconds:60, maxDeliveryCount:3, skipOlderMessagesInSeconds:3600, deadLetterQueueDestinationUUID:09914001-ad7c-4b10-94bf-44f3470dcb30, destinationUUID:8f1514e0-5372-4d78-8142-a2b98e0542a5, consumerGroupUUID:4d463085-3186-48bf-bfcc-b8b9ff8fc80c, ownerEmail:cherami-client-example@uber.com, isMultiZone:false)
consumer-0 started
publisher-0 started
publisher-0 stopped
Stopping publishers and consumers...
consumer-0: shutdown timed out

-------METRICS------------
messagesOut:              15734
bytesOut:                 16300424
messagesOutThrottled:     0
messagesOutErr:           5734
messagesIn:               10000
messagesInDups:           0
bytesIn:                  10360000
publisherMsgThroughput:   5000 msgs/sec
publisherBytesThroughput: 5431000 bytes/sec
consumerMsgThroughput:    3000 msgs/sec
consumerBytesThroughput:  3452000 bytes/sec
publishLatency (ms):      sum:3049339,avg:193,min:75,max:418
consumeLatency (ms):      sum:888,avg:0,min:0,max:75
Deleted ConsumerGroup /test/java.example_1872280763_reader
Deleted Destination /test/java.example_1872280763

