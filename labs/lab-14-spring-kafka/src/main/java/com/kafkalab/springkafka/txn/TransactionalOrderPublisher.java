package com.kafkalab.springkafka.txn;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The native mechanism this wraps: WP-09's hand-configured
 * transactional producer ({@code transactional.id}, {@code initTransactions},
 * {@code beginTransaction}/{@code commitTransaction}/{@code abortTransaction}).
 * {@code @Transactional} on this method begins a Kafka transaction (via
 * {@link TransactionalProducerConfig}'s dedicated
 * {@code KafkaTransactionManager}) on entry and commits it on a normal
 * return or aborts it if the method throws -- the EXACT same
 * commit-or-abort-atomically guarantee WP-09 built by hand, now
 * declarative.
 */
@Service
public class TransactionalOrderPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionalOrderPublisher(@Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional("kafkaTransactionManager")
    public void publish(String topic, String key, String value, boolean forceRollback) {
        kafkaTemplate.send(topic, key, value);
        if (forceRollback) {
            throw new IllegalStateException("forced rollback -- this send must never become visible under read_committed");
        }
    }
}
