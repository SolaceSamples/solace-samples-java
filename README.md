# solace-samples-java-new

## Overview
This repository holds code samples for the "Solace PubSub+ Messaging API for Java"  
*Note that this API is currently in **Early Access**. In order to download the API you need to register for the Solace Dev Community*

## Structure of Samples
There are two types of samples available: 
1. `samples/src/main/java/com/solace/patterns` demonstrates how to implement key message exchange patterns using the API. 
1. `samples/src/main/java/com/solace/sampler` demonstrates how to use specific features of the API.  

## Prerequisites
This tutorial requires the Solace Java API library. Download the Java API library to your computer from [here](https://solace.community/discussion/643/early-access-now-available-for-the-new-java-messaging-api).

**Place all JARs from within the two folders inside the zipfile of the EA distribution inside the `lib` folder so that Gradle can build.  This is temporary until all required JARs are in central repository. E.g.:**
```
commons-lang-2.6.jar
commons-logging-1.1.3.jar
org.apache.servicemix.bundles.jzlib-1.0.7_2.jar
org.osgi.annotation-6.0.0.jar
sol-common-10.9.1-nextGen.41309.jar
sol-jcsmp-10.9.1-nextGen.41309.jar
solace-messaging-client-0.0.5-dev.584.jar
```

## Building

```
./gradlew clean assemble
cd build/staged
./bin/DirectProcessor
```

### Import into Eclipse

```
./gradlew eclipse
```
Then import as Existing Gradle Project.


## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

See the list of [contributors](https://github.com/SolaceSamples/solace-samples-java-new/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.

## Resources

For more information try these resources:

- [API Tutorials](https://tutorials.solace.dev/)
- The Solace Developer Hub at: https://solace.dev
- Ask the [Solace Community.](http://dev.solace.com/community/)


