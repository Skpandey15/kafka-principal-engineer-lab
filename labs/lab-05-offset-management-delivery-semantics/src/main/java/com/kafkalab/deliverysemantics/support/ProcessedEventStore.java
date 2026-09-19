package com.kafkalab.deliverysemantics.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The idempotent-consumer building block WP-06 asks for: a durable record
 * of which {@code eventId}s this consumer group has already applied a
 * business side effect for, independent of Kafka's own offset storage.
 *
 * <p>This is deliberately a small, file-backed set rather than a database
 * or an "inbox table" -- the transactional-outbox and dual-write patterns
 * this lab's documentation discusses as the production-grade answer are
 * explicitly out of scope for WP-06 (see the lab README, "Production
 * considerations"). What this class demonstrates is the shape of the
 * idempotency check itself: before applying a side effect, ask "have I
 * already applied this eventId's effect?"; the answer must survive this
 * process crashing, or it isn't actually protecting anything -- an
 * in-memory {@code Set} would silently stop working the moment a crash
 * is simulated, which is exactly the scenario this lab needs to prove
 * duplicate-prevention across.
 *
 * <p>One store file is scoped to one {@code (groupId)} on disk, mirroring
 * how a real idempotency table would be scoped to a consumer group's
 * business logic, not to an individual consumer instance.
 */
public final class ProcessedEventStore {

    private final Path storeFile;
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    public ProcessedEventStore(Path storeFile) {
        this.storeFile = storeFile;
        load();
    }

    public static ProcessedEventStore forGroup(String groupId) {
        Path dir = Path.of(System.getProperty("java.io.tmpdir"), "kafka-lab-05-processed-events");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ProcessedEventStore(dir.resolve(groupId + ".txt"));
    }

    private void load() {
        if (!Files.exists(storeFile)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(storeFile, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    processedEventIds.add(trimmed);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** True if this eventId's business side effect has already been applied. */
    public boolean isProcessed(String eventId) {
        return processedEventIds.contains(eventId);
    }

    /**
     * Durably records that this eventId's side effect has now been applied.
     * Appends and flushes synchronously so the record survives a simulated
     * crash the instant after this call returns.
     */
    public void markProcessed(String eventId) {
        if (processedEventIds.add(eventId)) {
            try {
                Files.writeString(
                        storeFile,
                        eventId + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public int size() {
        return processedEventIds.size();
    }

    /** Test/experiment support: wipe this store's on-disk and in-memory state. */
    public void reset() {
        processedEventIds.clear();
        try {
            Files.deleteIfExists(storeFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
