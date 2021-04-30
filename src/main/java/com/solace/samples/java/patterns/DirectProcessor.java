package com.solace.samples.java.patterns;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceConstants.AuthenticationConstants;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.MessageProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.MessageReceiver.MessageHandler;
import com.solace.messaging.resources.Topic;
import com.solace.messaging.resources.TopicSubscription;

public class DirectProcessor {

	public static void main(String args[]) throws IOException {

		final Properties serviceConfiguration = new Properties();

		// connect to IP address 192.168.160.28 and port 55555 over TCP on the "default"
		// message VPN
		serviceConfiguration.setProperty(TransportLayerProperties.HOST, "tcp://localhost:55555");
		serviceConfiguration.setProperty(ServiceProperties.VPN_NAME, "default");
		// use basic auth
		serviceConfiguration.setProperty(AuthenticationProperties.SCHEME,
				AuthenticationConstants.AUTHENTICATION_SCHEME_BASIC);
		serviceConfiguration.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, "default");
		serviceConfiguration.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, "default");

		// Connect your MessagingService
		MessagingService msgSvc = MessagingService.builder(ConfigurationProfile.V1).fromProperties(serviceConfiguration)
				.build().connect();

		// Create your Receiver & Sender
		final DirectMessageReceiver receiver = msgSvc.createDirectMessageReceiverBuilder()
				.withSubscriptions(TopicSubscription.of("solace/samples/java/direct")).build().start();
		final DirectMessagePublisher publisher = msgSvc.createDirectMessagePublisherBuilder().build().start();

		// Create your Message Builder
		final OutboundMessageBuilder messageBuilder = msgSvc.messageBuilder().withProperty("PropForAllMessages",
				"value");

		// Process each message
		final MessageHandler messageHandler = (message) -> {
			// Access Message Headers & Properties
			final String destinationName = message.getDestinationName();
			String correlationId = (( correlationId = message.getCorrelationId()) != null) ? correlationId : UUID.randomUUID().toString() ;
			String customProperty = ((customProperty = message.getProperty("customProperty")) != null) ? customProperty
					: "No Custom Property";

			// Retrieve the Payload
			final String payload = message.getPayloadAsString();

			// Apply Business Logic
			System.out.println(correlationId + " Uppercasing Payload: " + payload);
			final String uppercasedPayload = payload.toUpperCase();

			// Set topic using business logic, can be dynamic!
			final Topic topic = Topic.of(destinationName + "/" + correlationId + "/uppercased");

			// Publish a processed Message w/ additional properties specific to the message
			final Properties additionalProperties = new Properties();
			additionalProperties.setProperty("customProperty", customProperty);
			additionalProperties.setProperty(MessageProperties.CORRELATION_ID, correlationId);

			final OutboundMessage outboundMessage = messageBuilder.build(uppercasedPayload);
			publisher.publish(outboundMessage, topic, additionalProperties);

		};
		receiver.receiveAsync(messageHandler);

		// Wait for user to quit
		System.out.println(DirectProcessor.class.getSimpleName() + " connected, and running. Press [ENTER] to quit.");
		Scanner input = new Scanner(System.in);
		System.out.print("Press Enter to quit...");
		input.nextLine();
		input.close();
		
		// Clean up Solace resources
		receiver.terminate(500);
		publisher.terminate(500);
		msgSvc.disconnect(); // will also clean up receiver and publisher
		System.out.println("Main thread quitting.");

	}

}
