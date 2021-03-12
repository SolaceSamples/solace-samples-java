package com.solace.sampler;


import com.solace.messaging.MessagingService;
import com.solace.messaging.config.TransportSecurityStrategy;
import com.solace.messaging.config.TransportSecurityStrategy.TLS;
import com.solace.messaging.config.TransportSecurityStrategy.TLS.SecureProtocols;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.util.SecureStoreFormat;

public class HowToConfigureTransportLayerSecurity {


  public static MessagingService tlsWithCertificateValidationAndTruststorePassword() {
    // enables acceptance of expired certificate
    final boolean ignoreExpiration = true;
    final TransportSecurityStrategy transportSecurity = TLS.create()
        .withCertificateValidation("myTruststorePassword", ignoreExpiration);
    return MessagingService.builder(ConfigurationProfile.V1).local()
        .withTransportSecurityStrategy(transportSecurity)
        .build().connect();
  }

  public static MessagingService tlsWithCertificateValidationAndTruststoreSettings() {
    // enables acceptance of expired certificate
    final boolean ignoreExpiration = true;
    final TransportSecurityStrategy transportSecurity = TLS.create()
        .withCertificateValidation("myTruststorePassword", ignoreExpiration, SecureStoreFormat.JKS,
            "/my/trustStore/FilePath");
    return MessagingService.builder(ConfigurationProfile.V1).local()
        .withTransportSecurityStrategy(transportSecurity)
        .build().connect();
  }

  public static MessagingService tlsDowngradableToPlainText() {
    final TransportSecurityStrategy transportSecurity = TLS.create().downgradable();
    return MessagingService.builder(ConfigurationProfile.V1).local()
        .withTransportSecurityStrategy(transportSecurity)
        .build().connect();
  }

  public static MessagingService tlsWithExcludedProtocols() {
    final TransportSecurityStrategy transportSecurity = TLS.create().withExcludedProtocols(
        SecureProtocols.SSLv3, SecureProtocols.TLSv1);
    return MessagingService.builder(ConfigurationProfile.V1).local()
        .withTransportSecurityStrategy(transportSecurity)
        .build().connect();
  }

}
