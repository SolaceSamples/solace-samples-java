package com.solace.samples.java.patterns;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.publisher.RequestReplyMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.Topic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class demonstrates the usage of the Solace Java API to create a Requester class.
 * This implementation focuses on the blocking behaviour of the API.
 * The mechanism of the Request-Reply pattern is defined in more detail over here : <a href="https://tutorials.solace.dev/jcsmp/request-reply/">Solace Request/Reply pattern</a>
 * <p>
 * Refer to the DirectReplierBlocking class for the reply component of the flow.
 */
public class DirectRequesterBlocking {

    private static final String SAMPLE_NAME = DirectRequesterBlocking.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples/";  // used as the topic "root"
    private static final String API = "Java";
    private static final int APPROX_MSG_RATE_PER_SEC = 100;
    private static final int PAYLOAD_SIZE = 100;
    private static volatile int msgSentCounter = 0;  // num messages sent
    private static volatile int loopCounter = 0;
    private static volatile boolean isShutdown = false;
    private static final long TIME_OUT_IN_MILLIS = 10000;

    public static void main(String... args) throws IOException {

        //1. Make sure that you have all the connection parameters.
        validateStartupParameters(args);

        System.out.println(API + " " + SAMPLE_NAME + " initializing...");

        //2. Set up the properties including username, password, vpnHostUrl and other control parameters.
        final Properties properties = new Properties();
        setupPropertiesForConnection(properties, args);

        //3. Create the MessagingService object and establishes the connection with the Solace event broker
        final MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1).fromProperties(properties).build();
        messagingService.connect();  // blocking connect

        //4. Register event handlers and callbacks for connection error handling.
        setupConnectivityHandlingInMessagingService(messagingService);

        //5. Build and start the publisher object
        final RequestReplyMessagePublisher requestReplyMessagePublisher = messagingService.requestReply().createRequestReplyMessagePublisherBuilder().build();
        requestReplyMessagePublisher.start();

        //6. Create a thread for printing message rate stats.
        registerLoggerForMonitoring();

        System.out.println(API + " " + SAMPLE_NAME + " connected, and running.");

        byte[] payload = new byte[PAYLOAD_SIZE];  // preallocate memory, for reuse, for performance
        String payloadInString;

        //7. Create the builder for the OutboundMessage
        final OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();

        while (System.in.available() == 0 && !isShutdown) {
            try {
                //8. Create the payload for each run of the loop
                char chosenCharacter = (char) (Math.round(loopCounter % 26) + 65);  // rotate through letters [A-Z]
                payloadInString = createMessagePayload(payload, chosenCharacter);

                //9. Create the OutboundMessage message with new payload, messageId and correlationId.
                final OutboundMessage outboundMessage = createOutboundMessageForPublishing(messageBuilder, payloadInString);

                //10. Define the topic name where the message will be posted.
                // This example defines a dynamic topic for each message to showcase Solace capability of wildcard based topics
                // In cases where dynamic topics names are not required, a simple topic string can be used
                final String topicString = new StringBuilder(TOPIC_PREFIX).append(API.toLowerCase()).append("/direct/request/").append(chosenCharacter).toString();  // StringBuilder faster than +

                System.out.println("The outbound message being published is : " + outboundMessage.getPayloadAsString());

                //11. Publishes the message in a blocking manner.
                // The API expects back a reply message from the Replier defined in the DirectReplierBlocking.java which is passed back in the InboundMessage object.
                final InboundMessage inboundMessage = requestReplyMessagePublisher.publishAwaitResponse(outboundMessage, Topic.of(topicString), TIME_OUT_IN_MILLIS);
                System.out.println("The reply inboundMessage being logged is : " + inboundMessage.dump());
                System.out.println("The reply inboundMessage payload being logged is : " + inboundMessage.getPayloadAsString());

                msgSentCounter++;  // increment by one
                loopCounter++;     // increment by one

            } catch (final RuntimeException | InterruptedException runtimeException) {
                System.out.printf("### Caught while trying to publisher.publish(): %s%n", runtimeException);
                isShutdown = true;  // or try to handle the specific exception more gracefully
            } finally {
                try {
                    Thread.sleep(1000 / APPROX_MSG_RATE_PER_SEC);  // do Thread.sleep(0) for max speed
                    // Note: STANDARD Edition Solace PubSub+ broker is limited to 10k msg/s max ingress
                } catch (InterruptedException e) {
                    isShutdown = true;
                }
            }
        }
    }

    private static void validateStartupParameters(final String... args) {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
            System.exit(-1);
        }
    }

    private static void setupPropertiesForConnection(final Properties properties, final String... args) {
        properties.setProperty(SolaceProperties.TransportLayerProperties.HOST, args[0]);          // host:port
        properties.setProperty(SolaceProperties.ServiceProperties.VPN_NAME, args[1]);     // message-vpn
        properties.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_USER_NAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(SolaceProperties.TransportLayerProperties.RECONNECTION_ATTEMPTS, "20");  // recommended settings
        properties.setProperty(SolaceProperties.TransportLayerProperties.CONNECTION_RETRIES_PER_HOST, "5");
    }

    private static void setupConnectivityHandlingInMessagingService(final MessagingService messagingService) {
        messagingService.addServiceInterruptionListener(serviceEvent -> {
            System.out.println("### SERVICE INTERRUPTION: " + serviceEvent.getCause());
            //isShutdown = true;
        });
        messagingService.addReconnectionAttemptListener(serviceEvent -> System.out.println("### RECONNECTING ATTEMPT: " + serviceEvent));
        messagingService.addReconnectionListener(serviceEvent -> System.out.println("### RECONNECTED: " + serviceEvent));
    }

    private static void registerLoggerForMonitoring() {
        final ScheduledExecutorService statsPrintingThread = Executors.newSingleThreadScheduledExecutor();
        statsPrintingThread.scheduleAtFixedRate(() -> {
            System.out.printf("Published msgs/s: %,d%n", msgSentCounter);  // simple way of calculating message rates
            msgSentCounter = 0;
        }, 1, 1, TimeUnit.SECONDS);
    }

    private static String createMessagePayload(byte[] payload, char chosenCharacter) {
        // each loop, change the payload, less trivial
        Arrays.fill(payload, (byte) chosenCharacter);  // fill the payload completely with that char
        return new String(payload);
    }

    private static OutboundMessage createOutboundMessageForPublishing(final OutboundMessageBuilder messageBuilder, final String payloadString) {
        final String header_id = UUID.randomUUID().toString();
        messageBuilder.withProperty(SolaceProperties.MessageProperties.APPLICATION_MESSAGE_ID, header_id);  // as an example of a header
        messageBuilder.withProperty(SolaceProperties.MessageProperties.CORRELATION_ID, header_id);  // as an example of a header
        return messageBuilder.build(payloadString);  // binary payload message
    }
}
