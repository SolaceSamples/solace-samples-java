package com.solace.samples.java.snippets;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.ReplayStrategy;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.InboundMessage.ReplicationGroupMessageId;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import java.time.ZonedDateTime;

public class HowToTriggerReplayAndConsumePersistentMessage {

  /**
   * Showcase for API to trigger replay of all available for replay messages
   *
   * @param service            ready configured and connected service instance
   * @param queueToConsumeFrom queue to consume from messages
   */
  public static void requestReplayOfAllAvailableMessages(MessagingService service,
      Queue queueToConsumeFrom) {
    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.allMessages()).build(queueToConsumeFrom).start();

    final InboundMessage message = receiver.receiveMessage();

  }

  /**
   * Showcase for API to trigger replay  based on a {@link ZonedDateTime}
   *
   * @param service            ready configured and connected service instance
   * @param queueToConsumeFrom queue to consume from messages
   * @param dateReplayFrom     timezone aware replay date
   */
  public static void requestReplayFromDate(
      MessagingService service, Queue queueToConsumeFrom, ZonedDateTime dateReplayFrom) {

    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(ReplayStrategy.timeBased(dateReplayFrom))
        .build(queueToConsumeFrom)
        .start();

    final InboundMessage message = receiver.receiveMessage();

  }


  /**
   * Showcase for API to retrieve ReplicationGroupMessageId in a string format. This string can be
   * stored in between to be restored to the ReplicationGroupMessageId object later and used in the
   * api to trigger message replay or it can also be used for administratively triggered message
   * replay via SEMP or CLI interface
   *
   * @param previouslyReceivedMessage previously received message to retrieve replication group
   *                                  message Id from
   * @return String representation of replication group message Id
   */
  public static String getReplicationGroupMessageIdStringFromInboundMessage(
      InboundMessage previouslyReceivedMessage) {

    final ReplicationGroupMessageId originalReplicationGroupMessageId = previouslyReceivedMessage
        .getReplicationGroupMessageId();
    return originalReplicationGroupMessageId.toString();
  }

  /**
   * Showcase for API to trigger message replay using string representation of a replication group
   * message Id
   *
   * @param service                           ready configured and connected service instance
   * @param queueToConsumeFrom                queue to consume from messages
   * @param replicationGroupMessageIdToString string representation of a replication group message
   *                                          Id
   */
  public static void requestReplayFromReplicationGroupMessageIdAsString(MessagingService service,
      Queue queueToConsumeFrom, String replicationGroupMessageIdToString) {

    // restored ReplicationGroupMessageId which can be used to configure Message Replay
    final ReplicationGroupMessageId restoredReplicationGroupMessageId = ReplicationGroupMessageId
        .of(replicationGroupMessageIdToString);

    // use restored ReplicationGroupMessageId object to configure Message Replay
    final PersistentMessageReceiver receiver = service
        .createPersistentMessageReceiverBuilder()
        .withMessageReplay(
            ReplayStrategy.replicationGroupMessageIdBased(restoredReplicationGroupMessageId))
        .build(queueToConsumeFrom)
        .start();

    final InboundMessage message = receiver.receiveMessage();

  }


}
