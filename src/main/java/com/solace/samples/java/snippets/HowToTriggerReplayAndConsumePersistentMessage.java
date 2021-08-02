package com.solace.samples.java.snippets;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.ReplayStrategy;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import java.time.ZonedDateTime;

public class HowToTriggerReplayAndConsumePersistentMessage {


  public static void requestReplayOfAllAvailableMessagesAndConsume(MessagingService service,
      Queue queueToConsumeFrom) {
    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.allMessages()).build(queueToConsumeFrom).start();

    final InboundMessage message = receiver.receiveMessage();

    receiver.ack(message);

  }

  public static void requestReplayFromDateAndConsume(
      MessagingService service, Queue queueToConsumeFrom, ZonedDateTime dateReplayFrom) {

    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.timeBased(dateReplayFrom))
        .build(queueToConsumeFrom)
        .start();

    final InboundMessage message = receiver.receiveMessage();

    receiver.ack(message);

  }


}
