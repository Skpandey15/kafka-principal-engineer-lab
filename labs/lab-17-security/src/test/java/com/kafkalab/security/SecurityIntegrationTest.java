package com.kafkalab.security;

import com.kafkalab.security.support.SecureClientProps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real TLS + real SASL/SCRAM-SHA-512 authentication + real
 * StandardAuthorizer ACL enforcement, against the ALREADY-RUNNING
 * {@code platform/kafka-security/} environment -- see the lab README,
 * "Why not Testcontainers here," for why (the same reasoning WP-16
 * already established): this WP's subject is real, hard-to-reprovision
 * infrastructure (a real CA, real SCRAM credentials baked in at
 * storage-format time), not client-library behavior alone.
 */
class SecurityIntegrationTest {

    @Test
    void anAuthenticatedAdminCanProduceAndConsumeOverRealTlsAndSasl() throws Exception {
        String topic = "sec-admin-" + uniqueId();
        createTopic(topic, "admin", "admin-secret");

        produce(topic, "admin", "admin-secret", "hello-over-real-tls");
        List<String> values = consume(topic, "admin", "admin-secret", "sec-admin-group-" + uniqueId(), 1);

        assertEquals(List.of("hello-over-real-tls"), values,
                "a real produce/consume round trip must succeed over TLS + SASL/SCRAM for a super.user");
    }

    @Test
    void wrongPasswordForARealScramUserFailsAuthentication() {
        String topic = "sec-badpw-" + uniqueId();

        Map<String, Object> props = SecureClientProps.forUser("admin", "definitely-the-wrong-password");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ExecutionException ex = assertThrows(ExecutionException.class,
                    () -> producer.send(new ProducerRecord<>(topic, "k", "v")).get(15, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof SaslAuthenticationException,
                    "a real SCRAM authentication failure must surface as SaslAuthenticationException, got " + ex.getCause());
        }
    }

    @Test
    void aClientWithNoTruststoreFailsTheRealTlsHandshake() {
        String topic = "sec-notrust-" + uniqueId();

        Map<String, Object> props = SecureClientProps.forUserWithNoTrust("admin", "admin-secret");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            assertThrows(Exception.class,
                    () -> producer.send(new ProducerRecord<>(topic, "k", "v")).get(15, TimeUnit.SECONDS),
                    "a client with no truststore at all must fail the real TLS handshake -- trust verification must be genuinely enforced, not decorative");
        }
    }

    @Test
    void aReaderWithNoGrantedAclsIsDeniedProducing() throws Exception {
        String topic = "sec-noacl-" + uniqueId();
        createTopic(topic, "admin", "admin-secret");

        Map<String, Object> props = SecureClientProps.forUser("reader", "reader-secret");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ExecutionException ex = assertThrows(ExecutionException.class,
                    () -> producer.send(new ProducerRecord<>(topic, "k", "v")).get(15, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof TopicAuthorizationException,
                    "a real, non-super-user with zero ACLs must be denied -- got " + ex.getCause());
        }
    }

    @Test
    void writeAclAloneAllowsProducingButNotConsuming() throws Exception {
        String topic = "sec-writeonly-" + uniqueId();
        String groupId = "sec-writeonly-group-" + uniqueId();
        createTopic(topic, "admin", "admin-secret");

        grantAcl(topic, "reader", AclOperation.WRITE);
        grantAcl(topic, "reader", AclOperation.DESCRIBE);

        // WRITE succeeds -- the ACL grant works.
        produce(topic, "reader", "reader-secret", "written-by-reader");

        // But READ was never granted -- fine-grained ACL scoping means
        // WRITE access does NOT imply READ access, a real, easy-to-get-wrong
        // assumption this test makes concrete. A real finding building
        // this test: the denial surfaces as GroupAuthorizationException,
        // not TopicAuthorizationException -- Kafka checks GROUP
        // authorization (joining/fetching as this consumer group)
        // BEFORE it ever reaches topic-level READ authorization, so
        // with BOTH missing, the group check is the one that fires.
        Map<String, Object> consumerProps = SecureClientProps.forUser("reader", "reader-secret");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));
            assertThrows(GroupAuthorizationException.class, () -> {
                Instant deadline = Instant.now().plusSeconds(15);
                while (Instant.now().isBefore(deadline)) {
                    consumer.poll(Duration.ofMillis(500));
                }
            }, "WRITE+DESCRIBE alone must not grant READ -- consuming must still be denied");
        }
    }

    @Test
    void grantingReadAndGroupAclsThenAllowsConsuming() throws Exception {
        String topic = "sec-readgranted-" + uniqueId();
        String groupId = "sec-readgranted-group-" + uniqueId();
        createTopic(topic, "admin", "admin-secret");

        grantAcl(topic, "reader", AclOperation.WRITE);
        grantAcl(topic, "reader", AclOperation.DESCRIBE);
        grantAcl(topic, "reader", AclOperation.READ);
        grantGroupReadAcl(groupId, "reader");

        produce(topic, "reader", "reader-secret", "fully-authorized-message");
        List<String> values = consume(topic, "reader", "reader-secret", groupId, 1);

        assertEquals(List.of("fully-authorized-message"), values,
                "once READ (topic) and READ (group) are BOTH granted, the same reader identity that was denied before must now succeed");
    }

    @Test
    void revokingAnAclDeniesFurtherAccess() throws Exception {
        String topic = "sec-revoke-" + uniqueId();
        createTopic(topic, "admin", "admin-secret");
        grantAcl(topic, "reader", AclOperation.WRITE);
        grantAcl(topic, "reader", AclOperation.DESCRIBE);

        produce(topic, "reader", "reader-secret", "before-revoke");

        revokeAcl(topic, "reader", AclOperation.WRITE);

        Map<String, Object> props = SecureClientProps.forUser("reader", "reader-secret");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            waitUntil(() -> {
                try {
                    producer.send(new ProducerRecord<>(topic, "k", "after-revoke")).get(5, TimeUnit.SECONDS);
                    return false;
                } catch (Exception e) {
                    return e.getCause() instanceof TopicAuthorizationException;
                }
            }, Duration.ofSeconds(30));
        }
    }

    // --- test infrastructure --------------------------------------------

    private static String uniqueId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static Admin adminClient() {
        return Admin.create(SecureClientProps.forUser("admin", "admin-secret"));
    }

    private static void createTopic(String topic, String username, String password) throws Exception {
        try (Admin admin = adminClient()) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private static void grantAcl(String topic, String principal, AclOperation operation) throws Exception {
        try (Admin admin = adminClient()) {
            ResourcePattern resource = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
            AccessControlEntry entry = new AccessControlEntry("User:" + principal, "*", operation, AclPermissionType.ALLOW);
            admin.createAcls(List.of(new AclBinding(resource, entry))).all().get();
        }
    }

    private static void grantGroupReadAcl(String groupId, String principal) throws Exception {
        try (Admin admin = adminClient()) {
            ResourcePattern resource = new ResourcePattern(ResourceType.GROUP, groupId, PatternType.LITERAL);
            AccessControlEntry entry = new AccessControlEntry("User:" + principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
            admin.createAcls(List.of(new AclBinding(resource, entry))).all().get();
        }
    }

    private static void revokeAcl(String topic, String principal, AclOperation operation) throws Exception {
        try (Admin admin = adminClient()) {
            ResourcePatternFilter resourceFilter = new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.LITERAL);
            AclBindingFilter filter = new AclBindingFilter(resourceFilter,
                    new AccessControlEntry("User:" + principal, "*", operation, AclPermissionType.ALLOW).toFilter());
            admin.deleteAcls(List.of(filter)).all().get();
        }
    }

    private static void produce(String topic, String username, String password, String value) throws Exception {
        Map<String, Object> props = SecureClientProps.forUser(username, password);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "k", value)).get(15, TimeUnit.SECONDS);
        }
    }

    private static List<String> consume(String topic, String username, String password, String groupId, int minCount) throws Exception {
        Map<String, Object> props = SecureClientProps.forUser(username, password);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<String> collected = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(20);
            while (collected.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                records.forEach(r -> collected.add(r.value()));
            }
        }
        if (collected.size() < minCount) {
            throw new AssertionError("expected at least " + minCount + " records, got " + collected.size());
        }
        return collected;
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
