/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.samples.java.patterns;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.ReceiverProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.MessageReceiver.MessageHandler;
import com.solace.messaging.resources.TopicSubscription;
import java.io.IOException;
import java.util.Properties;

/**
 * A more performant sample that shows an application that publishes.
 */
public class DirectSubscriber {
    
    private static final String SAMPLE_NAME = DirectSubscriber.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    
    private static volatile int msgRecvCounter = 0;                   // num messages sent
    private static volatile boolean hasDetectedDiscard = false;  // detected any discards yet?
    private static volatile boolean isShutdown = false;          // are we done yet?

    /** Main method. */
    public static void main(String... args) throws IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(SAMPLE_NAME + " initializing...");

        final Properties properties = new Properties();
        properties.setProperty(TransportLayerProperties.HOST, args[0]);          // host:port
        properties.setProperty(ServiceProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(ReceiverProperties.DIRECT_SUBSCRIPTION_REAPPLY, "true");  // subscribe Direct subs after reconnect
        properties.setProperty(TransportLayerProperties.RECONNECTION_ATTEMPTS, "1");  // best practices
        properties.setProperty(TransportLayerProperties.CONNECTION_RETRIES_PER_HOST, "2");
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        
        final MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(properties)
                .build();
        messagingService.connect();  // blocking connect to the broker
        messagingService.addServiceInterruptionListener(serviceEvent -> {
            System.out.println("### SERVICE INTERRUPTION: "+serviceEvent.getCause());
            isShutdown = true;
        });
        messagingService.addReconnectionAttemptListener(serviceEvent -> {
            System.out.println("### RECONNECTING ATTEMPT: "+serviceEvent);
        });
        messagingService.addReconnectionListener(serviceEvent -> {
            System.out.println("### RECONNECTED: "+serviceEvent);
        });

        final DirectMessageReceiver receiver = messagingService.createDirectMessageReceiverBuilder()
                .withSubscriptions(TopicSubscription.of(TOPIC_PREFIX+"/direct/>"))
                .withSubscriptions(TopicSubscription.of(TOPIC_PREFIX+"/control/>"))
                .build();
        receiver.start();
        
        receiver.setReceiverFailureListener(failedReceiveEvent -> {
            System.out.println("### FAILED RECEIVE EVENT " + failedReceiveEvent);
        });

        final MessageHandler messageHandler = (inboundMessage) -> {
            // do not print anything to console... too slow!
            msgRecvCounter++;
            // since Direct messages, check if there have been any lost any messages
            if (inboundMessage.getMessageDiscardNotification().hasBrokerDiscardIndication() ||
                    inboundMessage.getMessageDiscardNotification().hasInternalDiscardIndication()) {
                // If the consumer is being over-driven (i.e. publish rates too high), the broker might discard some messages for this consumer
                // check this flag to know if that's happened
                // to avoid discards:
                //  a) reduce publish rate
                //  b) use multiple-threads or shared subscriptions for parallel processing
                //  c) increase size of consumer's D-1 egress buffers (check client-profile) (helps more with bursts)
                hasDetectedDiscard = true;  // set my own flag
            }
            if (inboundMessage.getDestinationName().endsWith("control/quit")) {  // special sample message
                System.out.println("QUIT message received, shutting down.");  // example of command-and-control w/msgs
                isShutdown = true;
            }
        };
        receiver.receiveAsync(messageHandler);

        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        try {
            while (System.in.available() == 0 && !isShutdown) {
                Thread.sleep(1000);  // wait 1 second
                System.out.printf("Received msgs/s: %,d%n",msgRecvCounter);  // simple way of calculating message rates
                msgRecvCounter = 0;
                if (hasDetectedDiscard) {
                    System.out.println("*** Egress discard detected *** : "
                            + SAMPLE_NAME + " unable to keep up with full message rate");
                    hasDetectedDiscard = false;  // only show the error once per second
                }
            }
        } catch (InterruptedException e) {
            // Thread.sleep() interrupted... probably getting shut down
        }
        isShutdown = true;
        receiver.terminate(500);
        messagingService.disconnect();
        System.out.println("Main thread quitting.");
    }
}
