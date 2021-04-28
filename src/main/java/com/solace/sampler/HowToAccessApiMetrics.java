package com.solace.sampler;


import com.solace.messaging.MessagingService;
import com.solace.messaging.util.Manageable.ApiMetrics;
import com.solace.messaging.util.Manageable.ApiMetrics.Metric;

/**
 * Sampler for usage of API metrics
 */
public class HowToAccessApiMetrics {

  /**
   * Example how to access individual API metrics using messaging service instance
   *
   * @param service connected instance of a messaging service, ready to be used
   */
  public static void accessIndividualApiMetrics(MessagingService service) {

    final ApiMetrics metrics = service.metrics();
    // Metric enum gives access to variety of metrics
    metrics.getValue(Metric.DIRECT_MESSAGES_SENT);
  }


  /**
   * Example how to reset API metrics using messaging service instance
   *
   * @param service connected instance of a messaging service, ready to be used
   */
  public static void resetApiMetrics(MessagingService service) {

    final ApiMetrics metrics = service.metrics();
    // resets all collected metrics since last reset/start
    metrics.reset();
  }

  /**
   * Example how to get String representation of all current API metrics using messaging service
   * instance
   *
   * @param service connected instance of a messaging service, ready to be used
   */
  public static void toStringApiMetrics(MessagingService service) {

    final ApiMetrics metrics = service.metrics();
    // line separated view on all metrics
    String s = metrics.toString();
  }

}
