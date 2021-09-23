/*
 * Copyright 2021 Solace Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.solace.samples.java.snippets;

//import com.solace.messaging.MessagingService.TransactionalMessagingService;

public class HowToUseLocalTransactions {
// Just for EA release
//  public static void consumeMultipleMessageAndCommitTransaction(MessagingService service,
//      Queue endpointToConsumeFrom) {
//    // SETUP: use a transactional service
//    final TransactionalMessagingService transactionalService = service.transactional();
//    // SETUP: use a transactional receiver
//    final TransactionalMessageReceiver transactionalReceiver = transactionalService
//        .createTransactionalMessageReceiverBuilder().build(endpointToConsumeFrom).start();
//    boolean doRollback = false;
//    try {
//
//      final List<InboundMessage> messageCollector = new LinkedList<>();
//      // i.e assume that we would like to collect 5 'good' messages,
//      // and do something once we have all of them in a same transaction
//      int collected = 0;
//      int total = 0;
//      while (true) {
//        final InboundMessage messageInTransaction = transactionalReceiver.receiveMessage();
//        total++;
//        // i.e when message has payload it is considered to be good...
//        if (messageInTransaction != null &&
//            messageInTransaction.getPayloadAsBytes() != null) {
//          // collect 5 good messages
//          messageCollector.add(messageInTransaction);
//          if (++collected == 5) {
//            break;
//          }
//        }
//      }
//      // do some work here, that may throw an exception....
//      // doWork(...);
//    } catch (
//        PubSubPlusClientException e) {
//      // something went wrong, consider rollback, some especial handling based on exception
//      doRollback = true;
//    } catch (
//        Exception e) {
//      // something went wrong, consider rollback, some especial handling based on exception
//      doRollback = true;
//    } finally {
//      // commits transaction, PubSub+ Broker can delete all messages received within a committed transaction
//      if (!doRollback) {
//        // commit performs 'commit' (a kind of group ack) of ALL messages received within a transaction (#total) AND not just on collected (#collected)
//        // WARNING: messages which were not collected/swallowed are also committed
//        transactionalService.commit();
//      } else {
//        // rollback performs 'un-receive' of ALL messages received within a transaction (#total) AND not just on collected (#collected) ,
//        // WARNING: messages which were not collected/swallowed are also rolled back
//        transactionalService.rollback();
//      }
//      // an another new transaction started from here on
//      // Important: a NEW transaction starts automatically when previous one is finished by commit/rollback
//    }
//
//  }
//
//
//  public static void consumeMessagePublishAndCommitTransaction(MessagingService service,
//      Queue endpointToConsumeFrom) {
//    // SETUP: use a transactional service
//    final TransactionalMessagingService transactionalService = service
//        .transactional();
//    // SETUP: use a transactional receiver
//    final TransactionalMessageReceiver transactionalReceiver = transactionalService
//        .createTransactionalMessageReceiverBuilder().build(endpointToConsumeFrom).start();
//    // SETUP: use a transactional publisher
//    final TransactionalMessagePublisher publisher = transactionalService
//        .createTransactionalMessagePublisherBuilder().build();
//    boolean doRollback = false;
//    final Topic myDestination = Topic.of("example/myTopic");
//
//    try {
//      // receive a message in transaction
//      final InboundMessage messageReceivedInTransaction = transactionalReceiver.receiveMessage();
//
//      final OutboundMessage outboundMessageInTransaction = service.messageBuilder()
//          .build("message received");
//
//      // publishing message still within a same transaction as an inbound message was received
//      publisher.publish(outboundMessageInTransaction, myDestination);
//    } catch (PubSubPlusClientException e) {
//      // something went wrong, consider rollback
//      doRollback = true;
//    } finally {
//      if (!doRollback) {
//        // commit transaction (includes 2 messages: in and outbound )
//        transactionalService.commit();
//      } else {
//        // rollback transaction (includes 2 messages: in and outbound )
//        transactionalService.rollback();
//      }
//
//      // a new transaction started from here on
//      // Important: a NEW transaction starts automatically when previous one is finished by commit/rollback
//    }
//  }

}
