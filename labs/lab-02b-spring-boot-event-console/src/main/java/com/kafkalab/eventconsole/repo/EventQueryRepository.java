package com.kafkalab.eventconsole.repo;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import com.kafkalab.eventconsole.model.EventDocument;

@Repository
public class EventQueryRepository {

    public record Page(List<EventDocument> items, long total, int page, int size) {
    }

    private record PartitionCount(Integer id, long count) {
    }

    private final MongoTemplate mongo;

    public EventQueryRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /** Idempotent insert: returns false (and stores nothing) if this exact (topic, partition, offset) is already there. */
    public boolean insertIfAbsent(EventDocument doc) {
        try {
            mongo.insert(doc);
            return true;
        } catch (org.springframework.dao.DuplicateKeyException e) {
            return false;
        }
    }

    /**
     * One unordered bulk write for a whole consumed batch. Duplicates (MongoDB error
     * 11000: a redelivered record whose _id already exists) are expected and ignored;
     * any OTHER write error is a real failure and is rethrown so the batch is retried.
     * Unordered matters: one duplicate must not stop the rest of the batch from landing.
     *
     * @return how many documents were newly inserted
     */
    public int insertAllIgnoringDuplicates(List<EventDocument> docs) {
        if (docs.isEmpty()) {
            return 0;
        }
        try {
            return mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, EventDocument.class)
                    .insert(docs)
                    .execute()
                    .getInsertedCount();
        } catch (BulkOperationException e) {
            boolean onlyDuplicates = e.getErrors().stream().allMatch(err -> err.getCode() == DUPLICATE_KEY);
            if (!onlyDuplicates) {
                throw e;
            }
            return docs.size() - e.getErrors().size();
        } catch (org.springframework.dao.DuplicateKeyException e) {
            return 0;
        }
    }

    private static final int DUPLICATE_KEY = 11000;

    public Page search(String key, Integer partition, String text, int page, int size) {
        Criteria criteria = new Criteria();
        List<Criteria> parts = new java.util.ArrayList<>();
        if (key != null && !key.isBlank()) {
            parts.add(Criteria.where("key").regex(Pattern.quote(key.trim()), "i"));
        }
        if (partition != null) {
            parts.add(Criteria.where("partition").is(partition));
        }
        if (text != null && !text.isBlank()) {
            parts.add(Criteria.where("value").regex(Pattern.quote(text.trim()), "i"));
        }
        if (!parts.isEmpty()) {
            criteria = new Criteria().andOperator(parts);
        }

        Query countQuery = Query.query(criteria);
        long total = mongo.count(countQuery, EventDocument.class);

        Query query = Query.query(criteria)
                .with(Sort.by(Sort.Order.desc("consumedAt"), Sort.Order.desc("offset")))
                .with(PageRequest.of(page, size));
        List<EventDocument> items = mongo.find(query, EventDocument.class);
        return new Page(items, total, page, size);
    }

    public long count() {
        return mongo.count(new Query(), EventDocument.class);
    }

    /** Stored events per partition, partitions ascending. */
    public Map<String, Long> countByPartition() {
        Aggregation agg = Aggregation.newAggregation(Aggregation.group("partition").count().as("count"));
        Map<String, Long> out = new TreeMap<>(java.util.Comparator.comparingInt(Integer::parseInt));
        mongo.aggregate(agg, EventDocument.class, PartitionCount.class)
                .getMappedResults()
                .forEach(r -> out.put(String.valueOf(r.id()), r.count()));
        return out;
    }

    public long deleteAll() {
        return mongo.remove(new Query(), EventDocument.class).getDeletedCount();
    }
}
