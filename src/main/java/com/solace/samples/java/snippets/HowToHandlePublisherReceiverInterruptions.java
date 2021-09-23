package com.solace.samples.java.snippets;

import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.util.LifecycleControl.TerminationNotificationListener;

/**
 * Sampler for handling of receiver and publisher interruption/termination cases
 */

public class HowToHandlePublisherReceiverInterruptions {

  /**
   * Example how to configure persistent receiver with a listener for notifications about
   * unsolicited interruptions/terminations
   *
   * @param receiver message receiver (can be started or not)
   */
  public static void configureTerminationNotificationListenerOnPersistentReceiver(
      PersistentMessageReceiver receiver) {

    final TerminationNotificationListener terminationNotificationListener = (terminationEvent) -> {
      final String message = terminationEvent.getMessage();
      final long timestamp = terminationEvent.getTimestamp();
      final PubSubPlusClientException e = terminationEvent.getCause();
    };

    receiver.setTerminationNotificationListener(terminationNotificationListener);
  }


  /**
   * Example how to configure direct receiver with a listener for notifications about unsolicited
   * interruptions
   *
   * @param receiver message receiver (can be started or not)
   */
  public static void configureTerminationNotificationListenerOnDirectReceiver(
      DirectMessagePublisher receiver) {

    final TerminationNotificationListener terminationNotificationListener = (terminationEvent) -> {
      final String message = terminationEvent.getMessage();
      final long timestamp = terminationEvent.getTimestamp();
      final PubSubPlusClientException e = terminationEvent.getCause();
    };

    receiver.setTerminationNotificationListener(terminationNotificationListener);
  }

}
