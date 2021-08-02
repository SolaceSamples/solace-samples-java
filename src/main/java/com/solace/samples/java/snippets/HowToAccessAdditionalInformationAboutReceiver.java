package com.solace.samples.java.snippets;


import com.solace.messaging.MessagingService;
import com.solace.messaging.config.MissingResourcesCreationConfiguration.MissingResourcesCreationStrategy;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import com.solace.messaging.resources.TopicSubscription;
import com.solace.messaging.util.ManageableReceiver.PersistentReceiverInfo;
import com.solace.messaging.util.ManageableReceiver.PersistentReceiverInfo.ResourceInfo;

/**
 * Sampler for usage of Receiver Info api for
 */
public class HowToAccessAdditionalInformationAboutReceiver {

  /**
   * Example how to access access additional info about Persistent Receiver instance
   *
   * @param service connected instance of a messaging service, ready to be used
   */
  public static void accessAdditionalInfoAboutPersistentReceiver(MessagingService service) {

    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withSubscriptions(TopicSubscription.of("setSubscriptionExpressionHere"))
        .withMissingResourcesCreationStrategy(MissingResourcesCreationStrategy.CREATE_ON_START)
        // use convenience static factory methods in Queue to create NON durable anonymous exclusive queue
        .build(Queue.nonDurableExclusiveQueue()).start();

    // most of additional receiver info collected at runtime
    final PersistentReceiverInfo receiverInfo = receiver.receiverInfo();
    final long receiverInstanceId = receiverInfo.getId();
    final String receiverInstanceName = receiverInfo.getInstanceName();
    // provides access at the runtime to information about bonded queue
    final ResourceInfo infoAboutBondedQueue = receiverInfo.getResourceInfo();
    //  At the build time queue name is unknown; after receiver is connected, router creates a name which is accessible using QueueInfo#getName()
    final String queueNameAssignedFromRouter = infoAboutBondedQueue.getName();

  }


}
