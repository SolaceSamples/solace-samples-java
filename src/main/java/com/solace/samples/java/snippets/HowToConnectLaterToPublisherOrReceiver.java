package com.solace.samples.java.snippets;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.resources.TopicSubscription;

public class HowToConnectLaterToPublisherOrReceiver {

  public static void connectDirectMessageReceiverLater() {

    MessagingService service = MessagingService.builder(ConfigurationProfile.V1)
        .local().build("myApp").connect();

    final DirectMessageReceiver receiver = service
        .createDirectMessageReceiverBuilder()
        .withSubscriptions(TopicSubscription.of("addSubscriptionExpressionHere"))
        .build();

    // some time later
    receiver.start();
    // ready to do the job

  }


}
