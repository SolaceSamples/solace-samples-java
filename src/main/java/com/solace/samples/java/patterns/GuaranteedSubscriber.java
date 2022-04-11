/*
 * Copyright 2021-2022 Solace Corporation. All rights reserved.
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

package com.solace.samples.java.patterns;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import com.solace.messaging.resources.TopicSubscription;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.XMLMessageListener;

public class GuaranteedSubscriber {

    private static final String SAMPLE_NAME = GuaranteedSubscriber.class.getSimpleName();
    private static final String QUEUE_NAME = "q_java_sub";
    private static final String API = "Java";
    
    private static volatile int msgRecvCounter = 0;                 // num messages received
    private static volatile boolean hasDetectedRedelivery = false;  // detected any messages being redelivered?
    private static volatile boolean isShutdown = false;             // are we done?
    private static FlowReceiver flowQueueReceiver;

    // remember to add log4j2.xml to your classpath
    private static final Logger logger = LogManager.getLogger();  // log4j2, but could also use SLF4J, JCL, etc.

    /** This is the main app.  Use this type of app for receiving Guaranteed messages (e.g. via a queue endpoint). */
    public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(API + " " + SAMPLE_NAME + " initializing...");

        final Properties properties = new Properties();
        properties.setProperty(TransportLayerProperties.HOST, args[0]);          // host:port
        properties.setProperty(ServiceProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, args[3]);  // client-password
        }
        //properties.setProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS, true);  // not required, but interesting
        properties.setProperty(TransportLayerProperties.RECONNECTION_ATTEMPTS, "20");  // recommended settings
        properties.setProperty(TransportLayerProperties.CONNECTION_RETRIES_PER_HOST, "5");
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        // ready to connect now
        final MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(properties)
                .build();
        messagingService.connect();  // blocking connect
        messagingService.addServiceInterruptionListener(serviceEvent -> {
            logger.info("### SERVICE INTERRUPTION: "+serviceEvent.getCause());
            //isShutdown = true;
        });
        messagingService.addReconnectionAttemptListener(serviceEvent -> {
            logger.info("### RECONNECTING ATTEMPT: "+serviceEvent);
        });
        messagingService.addReconnectionListener(serviceEvent -> {
            logger.info("### RECONNECTED: "+serviceEvent);
        });
        
        
        final PersistentMessageReceiver receiver = messagingService
                .createPersistentMessageReceiverBuilder()
                // add the subscription to it, but don't try to automatically create the queue
                .withSubscriptions(TopicSubscription.of(GuaranteedNonBlockingPublisher.TOPIC_PREFIX+"pers/>"))
                // use convenience static factory Queues to create durable exclusive queue
                .build(Queue.durableExclusiveQueue(QUEUE_NAME)).start();

        receiver.receiveAsync((message) -> {
          // do something with my message, i.e check if it is not expired
          if (message != null && Instant.now().toEpochMilli() > message.getExpiration() &&
              message.getPayloadAsBytes() != null) {
            //and do ack
            receiver.ack(message);
          }

        });

        


            
        // tell the broker to start sending messages on this queue receiver
        flowQueueReceiver.start();
        // async queue receive working now, so time to wait until done...
        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        while (System.in.available() == 0 && !isShutdown) {
            Thread.sleep(1000);  // wait 1 second
            System.out.printf("%s %s Received msgs/s: %,d%n",API,SAMPLE_NAME,msgRecvCounter);  // simple way of calculating message rates
            msgRecvCounter = 0;
            if (hasDetectedRedelivery) {  // try shutting -> enabling the queue on the broker to see this
                System.out.println("*** Redelivery detected ***");
                hasDetectedRedelivery = false;  // only show the error once per second
            }
        }
        isShutdown = true;
        flowQueueReceiver.stop();
        Thread.sleep(1000);
        session.closeSession();  // will also close consumer object
        System.out.println("Main thread quitting.");
    }

    ////////////////////////////////////////////////////////////////////////////

    /** Very simple static inner class, used for receives messages from Queue Flows. **/
    private static class QueueFlowListener implements XMLMessageListener {

        @Override
        public void onReceive(BytesXMLMessage msg) {
            msgRecvCounter++;
            if (msg.getRedelivered()) {  // useful check
                // this is the broker telling the consumer that this message has been sent and not ACKed before.
                // this can happen if an exception is thrown, or the broker restarts, or the netowrk disconnects
                // perhaps an error in processing? Should do extra checks to avoid duplicate processing
                hasDetectedRedelivery = true;
            }
            // Messages are removed from the broker queue when the ACK is received.
            // Therefore, DO NOT ACK until all processing/storing of this message is complete.
            // NOTE that messages can be acknowledged from a different thread.
            msg.ackMessage();  // ACKs are asynchronous
        }

        @Override
        public void onException(JCSMPException e) {
            logger.warn("### Queue " + QUEUE_NAME + " Flow handler received exception.  Stopping!!", e);
            if (e instanceof JCSMPTransportException) {  // all reconnect attempts failed
                isShutdown = true;  // let's quit; or, could initiate a new connection attempt
            } else {
                // Generally unrecoverable exception, probably need to recreate and restart the flow
                flowQueueReceiver.close();
                // add logic in main thread to restart FlowReceiver, or can exit the program
            }
        }
    }
}
