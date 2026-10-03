package com.kafkalab.eventconsole.producer.repo;

import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import com.kafkalab.eventconsole.producer.model.PublishJob;

public interface PublishJobRepository extends MongoRepository<PublishJob, String> {

    List<PublishJob> findAllByOrderByCreatedAtDesc(Pageable pageable);
}
