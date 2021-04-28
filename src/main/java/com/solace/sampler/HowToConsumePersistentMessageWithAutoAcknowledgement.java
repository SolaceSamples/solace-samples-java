package com.solace.sampler;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.MessageReceiver.MessageHandler;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;

public class HowToConsumePersistentMessageWithAutoAcknowledgement {


  public static void consumeFullMessageAndDoAck(MessagingService service,
      Queue queueToConsumeFrom) {
    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageAutoAcknowledgement().build(queueToConsumeFrom).start();

    final InboundMessage message = receiver.receiveMessage();
    //auto- acked by now...
  }


  public static void consumeFullMessageUsingCallbackAndDoAck(final MessagingService service,
      final Queue queueToConsumeFrom) {

    final MessageHandler messageHandler = (message) -> {
      // Auto-ack: when the method MessageHandler#onMessage(message) exits with NO exceptions
      // NO Auto-ack: when the method MessageHandler#onMessage(message)
      // exits with an exceptions or error
    };

    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageAutoAcknowledgement().build(queueToConsumeFrom).start();

    receiver.receiveAsync(messageHandler);

  }


}
