package com.solace.sampler;


import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;


/**
 * Sampler for usage of versioned configuration profile
 */
public class HowToUseVersionedConfigurationProfiles {


  /**
   * Example how to configure API with a specific configuration profile version
   *
   * @return connected instance of a messaging service, that uses first version of configuration
   * profile
   */
  public static MessagingService configureServiceWithConfigurationProfile() {

    return MessagingService.builder(ConfigurationProfile.V1).local().build().connect();
  }


}
