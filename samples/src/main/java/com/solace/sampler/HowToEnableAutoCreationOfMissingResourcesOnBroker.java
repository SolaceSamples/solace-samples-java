package com.solace.sampler;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.MissingResourcesCreationConfiguration.MissingResourcesCreationStrategy;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;

/**
 * This example demonstrates a feature for enabling of automatic missing resources (queue) creation
 * on a broker. When this feature is enabled it does not warranties resource creation; it enables an
 * attempt to create missing resource. This feature is NOT recommended for production, where ALL
 * broker resources are created from an administrator. Default value is {@link
 * MissingResourcesCreationStrategy#DO_NOT_CREATE}.
 */
public class HowToEnableAutoCreationOfMissingResourcesOnBroker {


  public static void enableAutoCreationOfMissingResources(MessagingService service,
      Queue queueToConsumeFromThatNotSetupYet) {

    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMissingResourcesCreationStrategy(
            MissingResourcesCreationStrategy.CREATE_ON_START)
        .build(queueToConsumeFromThatNotSetupYet)
        // when receiver is being started,
        // API will attempt to create queue when missing.
        // When attempt fails (i.e due to lack of permissions for example)
        // PubSubPlusClientException is thrown
        .start();

    // receive as usual, blocking waiting on a next message routed to the newly created queue
    final InboundMessage message = receiver.receiveMessage();

    //any time later can do ack so long receiver did not closed
    receiver.ack(message);

  }

}

