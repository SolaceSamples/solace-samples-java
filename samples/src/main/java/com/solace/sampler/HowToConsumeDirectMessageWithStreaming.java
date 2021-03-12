package com.solace.sampler;

import com.solace.messaging.MessagingService;
import com.solace.messaging.resources.TopicSubscription;

public class HowToConsumeDirectMessageWithStreaming {


  public static void consumeStreamOfMessages(MessagingService service,
      TopicSubscription directMessageTopic) {
    // TODO to be implemented later
    //    final DirectMessageStreamReceiver receiver = service
    //        .createDirectMessageReceiverBuilder().withSubscriptions(directMessageTopic)
    //        .stream().build().start();
    //
    //    receiver.stream((message, throwable) -> {
    //      // do something about message processing exception
    //    }).filter((message -> {
    //      // i.e keep only messages that we not redelivered
    //      return message.isRedelivered();
    //    })).forEach((message -> {
    //      // process direct message
    //    }));
  }


}
