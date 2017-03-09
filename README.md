Cherami Client for JVM 
=======================

Java client library for publishing/consuming messages to/from [Cherami](https://eng.uber.com/cherami/).

[![Build Status](https://travis-ci.org/uber/cherami-client-java.svg?branch=master)](https://travis-ci.org/uber/cherami-client-java) [![Coverage Status](https://coveralls.io/repos/uber/cherami-client-java/badge.svg?branch=master&service=github)](https://coveralls.io/github/uber/cherami-client-java?branch=master)
[![stable](http://badges.github.io/stability-badges/dist/stable.svg)](http://github.com/badges/stability-badges)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Using cherami-client-java

The cherami-client-java library supports publishing/consuming from Cherami in synchronous and asynchronous modes. The usage below uses the synchronous blocking API.

```java
 // Create the client object with the server ip and port
 CheramiClient client = new CheramiClient.Builder(serverIP, serverPort).build();
 
 // Create a destination for publishing messages to
 CreateDestinationRequest dstRequest = new CreateDestinationRequest();
 dstRequest.setPath(destinationPath);
 client.createDestination(dstRequest);
 
 // Create a consumer group for consuming messages from the destination
 CreateConsumerGroupRequest cgRequest = new CreateConsumerGroupRequest();
 cgRequest.setDestinationPath(destinationPath);
 cgRequest.setConsumerGroupName(consumerGroupName);
 client.createConsumerGroup(cgRequest);
 
 // Publish a message
 CreatePublisherRequest pRequest = new CreatePublisherRequest.Builder(destinationPath).build()
 CheramiPublisher publisher = client.createPublisher(pRequest);
 publisher.open();
 publisher.write(new PublisherMessage("hello".getBytes("UTF8")));
 publisher.close();
 
 // Consume a message
 CreateConsumerRequest cRequest = new CreateConsumerRequest.Builder(destinationPath, consumerGroupName).build()
 CheramiConsumer consumer = client.createConsumer(cRequest);
 consumer.open();
 CheramiDelivery delivery = consumer.read();
 System.out.println(new String(delivery.getMessage().getPayload().getData(), "UTF8"));
 consumer.close();
 
 client.close();
```

For details on how to use the library in an application, see the examples directory.

## Development
Build with `mvn clean package`

## Contributing to Cherami
If you are interested in contributing to cherami the best place to start would be this blog post [eng.uber.com/cherami](https://eng.uber.com/cherami/)

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes.

Documentation
--------------
[eng.uber.com/cherami](https://eng.uber.com/cherami/)

License
-------
MIT License, please see [LICENSE](https://github.com/uber/cherami-client-java/blob/master/LICENSE) for details.