package com.kafkalab.security.support;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds real SASL_SSL client config -- the same shape every real
 * client (produce, consume, admin) needs to talk to
 * {@code platform/kafka-security/}'s real broker: TLS (via the real
 * CA-signed cert's truststore) plus SASL/SCRAM-SHA-512 authentication.
 */
public final class SecureClientProps {

    private SecureClientProps() {
    }

    public static Map<String, Object> forUser(String username, String password) {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, LabConfig.bootstrapServers());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, LabConfig.truststorePath());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, LabConfig.storePassword());
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        return props;
    }

    /** Real credentials, but NO truststore at all -- for proving TLS trust verification is genuinely enforced, not decorative. */
    public static Map<String, Object> forUserWithNoTrust(String username, String password) {
        Map<String, Object> props = forUser(username, password);
        props.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        props.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        props.remove(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        return props;
    }
}
