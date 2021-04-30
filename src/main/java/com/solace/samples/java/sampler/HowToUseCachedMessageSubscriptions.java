package com.solace.sampler;

import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.PubSubPlusClientException.TimeoutException;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.resources.CachedTopicSubscription;
import com.solace.messaging.resources.TopicSubscription;

/**
 * Sampler for adding/removing cache subscriptions. Usage of cache subscriptions require
 * configuration and hosting of SolCache application.
 */
public class HowToUseCachedMessageSubscriptions {


  /**
   * An example how to create and add {@code CachedTopicSubscription} to receive a mix of live and
   * cached messages matching specified topic subscription.
   *
   * @param subscriptionExpression topic subscription expression
   * @param cacheName              name of the solace cache to retrieve from
   * @param cacheAccessTimeout     solace cache request timeout (in milliseconds)
   * @param receiver               ready configured and connected receiver
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed, i.e when cache
   *                                   request failed and subscription rolled back/removed
   * @throws IllegalStateException     if the service is not running
   */
  public static void createSubscriptionToReceiveCachedAndLiveMessages(String subscriptionExpression,
      String cacheName, long cacheAccessTimeout, DirectMessageReceiver receiver)
      throws InterruptedException, TimeoutException, PubSubPlusClientException {

    final CachedTopicSubscription subscriptionForLiveAndCachedMessages = CachedTopicSubscription
        .asAvailable(cacheName, TopicSubscription.of(subscriptionExpression), cacheAccessTimeout);

    // adds subscription and performs a solace cache request
    receiver.addSubscription(subscriptionForLiveAndCachedMessages);
    //subscription is added unless exception was thrown as a result of Solace cache request failure.
  }


  /**
   * An example how to create and add {@code CachedTopicSubscription} to receive latest messages.
   * <p>When no live messages are available, cached messages matching specified topic subscription
   * considered latest, live messages otherwise
   * <p>When live messages are available then cached messages are discarded.
   *
   * @param receiver               ready configured and connected receiver
   * @param subscriptionExpression topic subscription expression
   * @param cacheName              name of the solace cache to retrieve from
   * @param cacheAccessTimeout     solace cache request timeout (in milliseconds)
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed
   * @throws IllegalStateException     if the service is not running
   */
  public static void createSubscriptionToReceiveLatestMessages(DirectMessageReceiver receiver,
      String subscriptionExpression,
      String cacheName, long cacheAccessTimeout)
      throws InterruptedException, PubSubPlusClientException {

    final CachedTopicSubscription subscriptionForLatestOfCachedOrLiveMessages = CachedTopicSubscription
        .liveCancelsCached(cacheName, TopicSubscription.of(subscriptionExpression),
            cacheAccessTimeout);

    // adds subscription and performs a solace cache request
    receiver.addSubscription(subscriptionForLatestOfCachedOrLiveMessages);
  }

  /**
   * An example how to create and add {@code CachedTopicSubscription} to receive first cached
   * messages when available followed by live messages.
   * <p>Live messages will be queued until the
   * solace cache response is received. Queued live messages are delivered to the application after
   * the cached messages are delivered.
   *
   * @param receiver               ready configured and connected receiver
   * @param subscriptionExpression topic subscription expression
   * @param cacheName              name of the solace cache to retrieve from
   * @param cacheAccessTimeout     solace cache request timeout (in milliseconds)
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed
   * @throws IllegalStateException     if the service is not running
   */
  public static void createSubscriptionToReceiveCachedMessagesFirst(DirectMessageReceiver receiver,
      String subscriptionExpression,
      String cacheName, long cacheAccessTimeout)
      throws PubSubPlusClientException, InterruptedException {

    final CachedTopicSubscription subscriptionToReceiveCachedFollowedByLiveMessages = CachedTopicSubscription
        .cachedFirst(cacheName, TopicSubscription.of(subscriptionExpression), cacheAccessTimeout);

    // adds subscription and performs a solace cache request
    receiver.addSubscription(subscriptionToReceiveCachedFollowedByLiveMessages);
  }


  /**
   * An example how to create and add {@code CachedTopicSubscription} to receive first cached
   * messages when available, followed by live messages.
   * <p>Additional cached message properties such as max number of cached messages and age of a
   * message from cache can be specified.
   * <p>Live messages will be queued until the solace cache response is received.
   * <p>Queued live messages are delivered to the application after the cached messages are
   * delivered.
   *
   * @param receiver                    ready configured and connected receiver
   * @param subscriptionExpression      topic subscription expression
   * @param cacheName                   name of the solace cache to retrieve from
   * @param cacheAccessTimeout          solace cache request timeout (in milliseconds)
   * @param maxCachedMessages           the max number of messages expected to be received form a
   *                                    Solace cache
   * @param cachedMessageAge            the maximum age (in seconds) of the messages to retrieve
   *                                    from a Solace cache
   * @param ignoreCacheAccessExceptions {@code true} prevents Subscription rollback on Solace cache
   *                                    request errors. {@code false} is default value.
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed
   * @throws IllegalStateException     if the service is not running
   */
  public static void createCachedSubscriptionWithMoreOptions(DirectMessageReceiver receiver,
      String subscriptionExpression,
      String cacheName, long cacheAccessTimeout, int maxCachedMessages, int cachedMessageAge,
      boolean ignoreCacheAccessExceptions)
      throws InterruptedException, IllegalStateException, PubSubPlusClientException {

    final TopicSubscription topic = TopicSubscription.of(subscriptionExpression);

    final CachedTopicSubscription subscriptionToReceiveCachedFollowedByLiveMessages = CachedTopicSubscription
        .cachedFirst(cacheName, topic, maxCachedMessages, cachedMessageAge, cacheAccessTimeout,
            ignoreCacheAccessExceptions);
    // adds subscription and performs a solace cache request
    receiver.addSubscription(subscriptionToReceiveCachedFollowedByLiveMessages);

  }

  /**
   * An example how to create and add {@code CachedTopicSubscription} to receive cached messages
   * only, when available; no life messages are expected to be received.
   *
   * @param receiver               ready configured and connected receiver
   * @param subscriptionExpression topic subscription expression
   * @param cacheName              name of the solace cache to retrieve from
   * @param cacheAccessTimeout     solace cache request timeout (in milliseconds)
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed
   * @throws IllegalStateException     if the service is not running
   */
  public static void createSubscriptionToReceiveOnlyCachedMessages(DirectMessageReceiver receiver,
      String subscriptionExpression,
      String cacheName, long cacheAccessTimeout)
      throws PubSubPlusClientException, InterruptedException {

    final CachedTopicSubscription subscriptionToReceiveCachedFollowedByLiveMessages = CachedTopicSubscription
        .cachedOnly(cacheName, TopicSubscription.of(subscriptionExpression), cacheAccessTimeout);

    // adds subscription and performs a solace cache request
    receiver.addSubscription(subscriptionToReceiveCachedFollowedByLiveMessages);
  }


  /**
   * Example how to add cached topic subscriptions to a direct message receiver
   *
   * @param receiver                ready configured and connected receiver
   * @param cachedTopicSubscription cached topic subscription
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed
   * @throws IllegalStateException     if the service is not running
   */
  public static void addCachedSubscription(DirectMessageReceiver receiver,
      CachedTopicSubscription cachedTopicSubscription)
      throws InterruptedException, IllegalStateException, PubSubPlusClientException {
    // adds subscription and performs a solace cache request
    receiver.addSubscription(cachedTopicSubscription);
  }

  /**
   * Example how to remove cached topic subscriptions. Removal of cached topic subscriptions is
   * equivalent to removal of a regular {@code TopicSubscription}
   *
   * @param receiver           ready configured and connected receiver
   * @param cachedSubscription topic subscription to be removed
   * @throws InterruptedException      if any thread has interrupted the current thread
   * @throws PubSubPlusClientException if the operation could not be performed
   * @throws IllegalStateException     if the service is not running
   */
  public static void removeSubscription(DirectMessageReceiver receiver,
      CachedTopicSubscription cachedSubscription)
      throws InterruptedException, IllegalStateException, PubSubPlusClientException {

    // un- subscribe from a message source is analogous to a regular
    receiver.removeSubscription(cachedSubscription);
    // no more subscriptions
  }


  /**
   * Example how asynchronously to add cached topic subscriptions to direct message receiver
   *
   * @param receiver           ready configured and connected receiver
   * @param cachedSubscription cached topic subscription to be added
   */
  public static void addSubscriptionAsynchronous(DirectMessageReceiver receiver,
      CachedTopicSubscription cachedSubscription) {

    // 2. argument SubscriptionChangeListener us used to notify user about success of the operation
    receiver.addSubscriptionAsync(cachedSubscription, ((cachedTopicSubscription, operation,
        exception) -> {
      // operation failed
      if (exception != null) {
        // do something about failed add/remove cached subscription operation:
        //  switch (operation){
        //    case ADDED: doWhenAddSubscriptionFailed(topicSubscription);
        //   case REMOVED:doWhenRemoveSubscriptionFailed(topicSubscription);
      }

      // operation finished successfully
      else {
        // do something:
        //  switch (operation){
        //    case ADDED: doWhenSubscriptionAdded(topicSubscription);
        //   case REMOVED:doWhenSubscriptionRemoved(topicSubscription);
      }
    }
    ));

  }


}
