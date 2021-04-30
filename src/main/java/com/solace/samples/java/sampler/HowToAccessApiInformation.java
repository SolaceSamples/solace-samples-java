package com.solace.samples.java.sampler;


import com.solace.messaging.MessagingService;
import com.solace.messaging.util.Manageable.ApiInfo;

/**
 * Sampler for usage of API information
 */
public class HowToAccessApiInformation {

  /**
   * Example how to access API information using messaging service instance
   *
   * @param service instance of a messaging service
   */
  public static void accessApiInfo(MessagingService service) {

    final ApiInfo apiInfo = service.info();
    final String apiVersion = apiInfo.getApiVersion();
    final String apiBuildDate = apiInfo.getApiBuildDate();
    final String apiVendor = apiInfo.getApiImplementationVendor();
  }

}
