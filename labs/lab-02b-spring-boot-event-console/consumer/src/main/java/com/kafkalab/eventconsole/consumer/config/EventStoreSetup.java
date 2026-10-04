package com.kafkalab.eventconsole.consumer.config;

import java.time.Duration;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import com.kafkalab.eventconsole.consumer.model.EventDocument;
import com.kafkalab.eventconsole.consumer.model.EventStatus;

/**
 * Prepares the events collection before anything is consumed (it runs while the application
 * context is built, which is before the Kafka listeners start). All of it is idempotent, so any
 * number of instances can start at once and a restart changes nothing.
 *
 * <p><b>Why a partial TTL index.</b> The read model of SUCCESS events must not grow forever, so
 * they expire. A plain TTL index would expire EVERY document, including FAILED and DEAD ones --
 * the events that most need a human -- and would do it silently. This index only covers
 * {@code status = SUCCESS}, so unfinished events stay until they are resolved.
 */
@Component
public class EventStoreSetup implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(EventStoreSetup.class);

    public static final String TTL_INDEX = "ttl_success_consumedAt";
    /** The pre-status index: it expired every document regardless of status, so it must go. */
    public static final String LEGACY_TTL_INDEX = "ttl_consumedAt";
    private static final int MIGRATION_CHUNK = 5_000;

    private final MongoTemplate mongo;
    private final AppProperties props;

    public EventStoreSetup(MongoTemplate mongo, AppProperties props) {
        this.mongo = mongo;
        this.props = props;
    }

    @Override
    public void afterPropertiesSet() {
        // Order matters: with the unsafe index gone and the safe one in place, nothing can expire
        // an unfinished event while the status field is being back-filled.
        dropLegacyTtlIndex();
        ensureTtlIndex(props.eventsTtl());
        migrateLegacyEvents();
    }

    public void dropLegacyTtlIndex() {
        List<IndexInfo> indexes = mongo.indexOps(EventDocument.class).getIndexInfo();
        if (indexes.stream().anyMatch(i -> LEGACY_TTL_INDEX.equals(i.getName()))) {
            mongo.indexOps(EventDocument.class).dropIndex(LEGACY_TTL_INDEX);
            log.info("Dropped legacy index {} (it expired events of every status)", LEGACY_TTL_INDEX);
        }
    }

    public void ensureTtlIndex(Duration ttl) {
        IndexInfo existing = mongo.indexOps(EventDocument.class).getIndexInfo().stream()
                .filter(i -> TTL_INDEX.equals(i.getName())).findFirst().orElse(null);
        if (existing == null) {
            mongo.indexOps(EventDocument.class).createIndex(new Index()
                    .named(TTL_INDEX)
                    .on("consumedAt", Sort.Direction.ASC)
                    .expire(ttl)
                    .partial(PartialIndexFilter.of(Criteria.where("status").is(EventStatus.SUCCESS.name()))));
            log.info("Created {}: SUCCESS events expire after {}", TTL_INDEX, ttl);
        } else if (!existing.getExpireAfter().map(ttl::equals).orElse(false)) {
            // Changing the TTL of an existing index is a collMod, not a re-create (which MongoDB rejects).
            mongo.getDb().runCommand(new Document("collMod", mongo.getCollectionName(EventDocument.class))
                    .append("index", new Document("name", TTL_INDEX).append("expireAfterSeconds", ttl.toSeconds())));
            log.info("Changed {}: SUCCESS events now expire after {}", TTL_INDEX, ttl);
        }
    }

    /**
     * Events stored before status existed were all successfully stored, and without the field the
     * partial TTL index would never expire them. Back-fill the field once; later runs match nothing.
     */
    public long migrateLegacyEvents() {
        // In bounded chunks, not one update over the whole collection: a single updateMulti over hundreds of
        // thousands of documents pushes MongoDB's cache and journal hard enough to get a small container
        // OOM-killed (seen on the 232,000-event lab database), and a killed back-fill just starts over.
        long migrated = 0;
        String collection = mongo.getCollectionName(EventDocument.class);
        while (true) {
            Query chunk = Query.query(Criteria.where("status").exists(false)).limit(MIGRATION_CHUNK);
            chunk.fields().include("_id");
            // Raw documents: only _id is fetched, and the entity (which has primitive fields) cannot be built from that.
            List<Object> ids = mongo.find(chunk, Document.class, collection).stream().<Object>map(d -> d.get("_id")).toList();
            if (ids.isEmpty()) {
                break;
            }
            migrated += mongo.updateMulti(Query.query(Criteria.where("_id").in(ids).and("status").exists(false)),
                    new Update().set("status", EventStatus.SUCCESS.name()), EventDocument.class).getModifiedCount();
        }
        if (migrated > 0) {
            log.info("Back-filled status=SUCCESS on {} events stored before the status field existed", migrated);
        }
        return migrated;
    }
}
