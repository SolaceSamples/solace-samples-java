package com.solace.samples.java.snippets;


import com.solace.messaging.MessagingService;
import com.solace.messaging.config.AuthenticationStrategy.BasicUserNamePassword;
import com.solace.messaging.config.AuthenticationStrategy.ClientCertificateAuthentication;
import com.solace.messaging.config.AuthenticationStrategy.Kerberos;
import com.solace.messaging.config.TransportSecurityStrategy.TLS;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.util.SecureStoreFormat;
import java.util.Properties;

/**
 * Sampler for authentication configuration
 */
public class HowToConfigureAuthentication {

  /**
   * setup for basic auth using user name and password
   *
   * @param userName user name
   * @param password password
   * @return configured and connected instance of {@code MessagingService} ready to be used for
   * messaging tasks
   */
  public static MessagingService configureBasicAuthCredentials(String userName,
      String password) {
    return MessagingService.builder(ConfigurationProfile.V1).local()
        // can configure or override credentials manually
        .withAuthenticationStrategy(
            BasicUserNamePassword.of(userName, password))
        .build().connect();
  }

  /**
   * Prerequisite for this example to work on a client side is existence of jaas login configuration
   * file 'jaas.conf' similar to one in 'src/main/resources/' and valid kerberos configuration file
   * 'krb5.conf'
   *
   * @param serviceConfiguration service configuration properties
   * @return configured and connected instance of {@code MessagingService} ready to be used for
   * messaging tasks
   * @see <a href="https://docs.solace.com/Configuring-and-Managing/Configuring-Client-Authentication.htm#Config-Kerberos">How
   * to configure Kerberos</a>
   */
  public static MessagingService configureKerberosWithJaasLoginContextName(
      Properties serviceConfiguration) {

    final String jaasLoginContextName = "SolaceGSS";

    return MessagingService.builder(ConfigurationProfile.V1).fromProperties(serviceConfiguration)
        // can configure or override authentication manually
        .withAuthenticationStrategy(
            // jaasLoginContextName correlates to the entry in resources/jaas.conf file
            // uses default value 'solace' for kerberos principal instance name,
            // no mutual authentication is enabled
            Kerberos.of(jaasLoginContextName))
        .build().connect();
  }

  /**
   * Prerequisite for this example to work to work on a client is existence of jaas login
   * configuration file 'jaas.conf' similar to one in 'src/main/resources/' and valid kerberos
   * configuration file 'krb5.conf'
   *
   * @param serviceConfiguration service configuration properties
   * @return configured and connected instance of {@code MessagingService} ready to be used for
   * messaging tasks
   * @see <a href="https://docs.solace.com/Configuring-and-Managing/Configuring-Client-Authentication.htm#Config-Kerberos">How
   * to configure Kerberos</a>
   */
  public static MessagingService configureKerberosCustomizeAllSettings(
      Properties serviceConfiguration) {
    final String kerberosPrincipalInstanceName = "solace01";
    final String jaasLoginContextName = "SolaceGSS";
    final String myUserNameOnBroker = "myUserNameOnBroker";

    return MessagingService.builder(ConfigurationProfile.V1).fromProperties(serviceConfiguration)
        // can configure or override authentication manually
        .withAuthenticationStrategy(
            // jaasLoginContextName correlates to the entry in resources/jaas.conf file
            // myUserNameOnBroker should be setup on a broker,
            // enable mutual authentication and JAAS config file reloading
            Kerberos
                .of(kerberosPrincipalInstanceName, jaasLoginContextName)
                .withUserName(myUserNameOnBroker).withMutualAuthentication()
                .withReloadableJaasConfiguration())
        .build().connect();
  }


  /**
   * For a client to use a client certificate authentication scheme, the host event broker must be
   * properly configured for TLS/SSL connections, and Client Certificate Verification must be
   * enabled for the particular Message VPN that the client is connecting to. On client side client
   * certificate needs to be present in a keystore file.
   *
   * @param myKeystorePassword password for the keystore
   * @param myKeystoreUrl      url to the keystore file
   * @return configured and connected instance of {@code MessagingService} ready to be used for
   * messaging tasks
   */
  public static MessagingService configureClientCertificateAuthenticationCustomizeAllSettings(
      String myKeystorePassword, String myKeystoreUrl) {

    return MessagingService.builder(ConfigurationProfile.V1).localSecure(55443)
        // transport security TLS is REQUIRED
        .withTransportSecurityStrategy(TLS.create())
        // can configure or override authentication manually
        .withAuthenticationStrategy(
            // jaasLoginContextName correlates to the entry in resources/jaas.conf file
            // myUserNameOnBroker should be setup on a broker, or just use null
            ClientCertificateAuthentication
                .of(myKeystoreUrl, myKeystorePassword)
        )
        .build().connect();
  }

  /**
   * For a client to use a client certificate authentication scheme, the host event broker must be
   * properly configured for TLS/SSL connections, and Client Certificate Verification must be
   * enabled for the particular Message VPN that the client is connecting to. On client side client
   * certificate needs to be present in a keystore file.
   *
   * @param myKeystorePassword           password for the keystore
   * @param myEspecialPrivateKeyPassword password for the private key stored in a keystore if a
   *                                     different from a {@code myKeystorePassword} password is
   *                                     used
   * @param myKeystoreUrl                url to the keystore file
   * @return configured and connected instance of {@code MessagingService} ready to be used for
   * messaging tasks
   */
  public static MessagingService configureClientCertificateAuthenticationCustomizeAllSettings(
      String myKeystorePassword, String myEspecialPrivateKeyPassword, String myKeystoreUrl) {

    return MessagingService.builder(ConfigurationProfile.V1).localSecure(55443)
        // transport security TLS is REQUIRED
        .withTransportSecurityStrategy(TLS.create())
        // can configure or override authentication manually
        .withAuthenticationStrategy(
            // jaasLoginContextName correlates to the entry in resources/jaas.conf file
            // myUserNameOnBroker should be setup on a broker
            ClientCertificateAuthentication
                .of(myKeystoreUrl, myKeystorePassword, SecureStoreFormat.PKCS12)
                .withPrivateKeyPassword(myEspecialPrivateKeyPassword)
        )
        .build().connect();
  }


}
