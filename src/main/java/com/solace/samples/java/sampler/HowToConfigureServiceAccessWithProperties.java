package com.solace.samples.java.sampler;


import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceConstants.AuthenticationConstants;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import java.util.Properties;

/**
 * Sampler for messaging service access to a broker using properties
 */
public class HowToConfigureServiceAccessWithProperties {

  /**
   * Example how to configure service access using properties
   *
   * @return connected instance of a messaging service, ready to be used
   */
  public static MessagingService configureAndConnectUsingProperties() {

    final Properties serviceConfiguration = new Properties();

    //connect to IP address 192.168.160.28 and port 77777 over TCP
    serviceConfiguration.setProperty(TransportLayerProperties.HOST,
        "tcp:192.168.160.28:77777");
    // use basic auth
    serviceConfiguration.setProperty(AuthenticationProperties.SCHEME,
        AuthenticationConstants.AUTHENTICATION_SCHEME_BASIC);

    serviceConfiguration.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME,
        "aUser");

    serviceConfiguration.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD,
        "hadDtOGuESsPaSsW0rD");

    return MessagingService.builder(ConfigurationProfile.V1).fromProperties(serviceConfiguration)
        .build().connect();
  }


}
