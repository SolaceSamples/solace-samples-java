package com.solace.samples.java.snippets;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.resources.Topic;
import com.solace.messaging.resources.TopicSubscription;
import com.solace.messaging.util.MessageToSDTMapConverter;
import com.solace.messaging.util.SolaceSDTMap;
import com.solace.messaging.util.SolaceSDTMapToMessageConverter;
import com.solacesystems.jcsmp.SDTMap;

public class HowToWorkWithSolaceSDTTypesAndMessages {


  public static void consumeJustSDTMapPayload(MessagingService service) {

    final DirectMessageReceiver receiver = service
        .createDirectMessageReceiverBuilder()
        .withSubscriptions(TopicSubscription.of("subscriptionExpression"))
        .build().start();

    //SolaceSDTMap extends SDTMap
    SolaceSDTMap map = receiver.receiveMessage()
        .getAndConvertPayload(new MessageToSDTMapConverter(), SolaceSDTMap.class);

  }


  public static void publishSDTMap(SolaceSDTMap content,
      MessagingService service,
      Topic toDestination) {

    final DirectMessagePublisher messagePublisher = service.createDirectMessagePublisherBuilder()
        .build().start();

    messagePublisher
        .publish(service.messageBuilder().build(content, new SolaceSDTMapToMessageConverter()),
            toDestination);

  }

  public static OutboundMessage createMessageWithSDTMapPayload(SolaceSDTMap content,
      MessagingService service) {
    final OutboundMessage message = service.messageBuilder()
        .withHTTPContentHeader("text/plain", "UTF-8")
        .withPriority(100).build(content, new SolaceSDTMapToMessageConverter());
    return message;
  }


  public static SolaceSDTMap createSolaceSDTMap(SDTMap sdtMap, String someString,
      Integer someInteger) {
    final SolaceSDTMap map = new SolaceSDTMap();
    map.putMap("myMapKey123", sdtMap);
    map.putInteger("myIntegerKey123", someInteger);
    map.putString("myStringKey123", someString);
    // many more different types can be added to the SolaceSDTMap
    return map;
  }


}
