package com.kafkalab.retrydlq.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

/**
 * The full production upgrade over WP-06's {@code ProcessedEventStore}
 * (a single-process, file-backed {@code Set<String>} with a
 * check-then-act race -- see that class's own Javadoc for why it was
 * deliberately kept minimal): a real {@code processed_events} table
 * with a {@code PRIMARY KEY (group_id, event_id)} doing the
 * duplicate-detection work ATOMICALLY, via {@code INSERT ... ON
 * CONFLICT DO NOTHING}. Two concurrent attempts claiming the SAME
 * event_id can never both believe they own it -- the database itself
 * arbitrates, not application-level logic racing against itself.
 *
 * <p>Models a claim LIFECYCLE, not just a boolean "have I seen this":
 * {@link #tryClaim} (IN_PROGRESS), {@link #complete} (DONE), and
 * {@link #release} (deletes an IN_PROGRESS claim whose processing
 * ultimately failed, so a later, fixed redelivery isn't blocked
 * forever). {@link #tryClaim} also reclaims a STALE IN_PROGRESS claim
 * (one whose owning process crashed mid-processing and never called
 * {@code complete} or {@code release}) once it's older than
 * {@code leaseTimeout} -- see the conceptual doc's crash-scenario
 * section for why an unconditional "IN_PROGRESS forever blocks
 * reprocessing" design would be a real, silent correctness bug.
 */
public final class IdempotencyStore {

    /** What {@link #tryClaim} discovered about this event_id. */
    public enum ClaimResult {
        /** Newly claimed -- caller should process it and then call {@link #complete} or {@link #release}. */
        CLAIMED,
        /** Already DONE -- a true duplicate; skip processing entirely. */
        ALREADY_DONE,
        /** Currently IN_PROGRESS, and not yet stale -- another attempt owns it; skip. */
        IN_PROGRESS_ELSEWHERE
    }

    private final Connection connection;
    private final String groupId;

    public IdempotencyStore(Connection connection, String groupId) {
        this.connection = connection;
        this.groupId = groupId;
    }

    public ClaimResult tryClaim(String eventId, Duration leaseTimeout) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO processed_events (group_id, event_id, status, claimed_at) VALUES (?, ?, 'IN_PROGRESS', now()) "
                        + "ON CONFLICT (group_id, event_id) DO NOTHING")) {
            insert.setString(1, groupId);
            insert.setString(2, eventId);
            if (insert.executeUpdate() == 1) {
                return ClaimResult.CLAIMED;
            }
        }

        // Someone (possibly a now-dead process) already holds this
        // event_id -- inspect its status before giving up on it.
        String status;
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT status FROM processed_events WHERE group_id = ? AND event_id = ?")) {
            select.setString(1, groupId);
            select.setString(2, eventId);
            try (ResultSet rs = select.executeQuery()) {
                rs.next();
                status = rs.getString("status");
            }
        }
        if ("DONE".equals(status)) {
            return ClaimResult.ALREADY_DONE;
        }

        // IN_PROGRESS: attempt an atomic reclaim, but ONLY if the claim
        // is older than the lease window -- the WHERE clause itself is
        // the concurrency guard, so at most one of several racing
        // reclaim attempts can ever affect a row.
        try (PreparedStatement reclaim = connection.prepareStatement(
                "UPDATE processed_events SET claimed_at = now() "
                        + "WHERE group_id = ? AND event_id = ? AND status = 'IN_PROGRESS' AND claimed_at < now() - (? || ' milliseconds')::interval")) {
            reclaim.setString(1, groupId);
            reclaim.setString(2, eventId);
            reclaim.setLong(3, leaseTimeout.toMillis());
            if (reclaim.executeUpdate() == 1) {
                return ClaimResult.CLAIMED;
            }
        }
        return ClaimResult.IN_PROGRESS_ELSEWHERE;
    }

    public void complete(String eventId) throws Exception {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE processed_events SET status = 'DONE', completed_at = now() WHERE group_id = ? AND event_id = ?")) {
            update.setString(1, groupId);
            update.setString(2, eventId);
            update.executeUpdate();
        }
    }

    /** Releases a claim whose processing ultimately failed (routed to the DLQ) -- only ever deletes an IN_PROGRESS row, never a DONE one. */
    public void release(String eventId) throws Exception {
        try (PreparedStatement delete = connection.prepareStatement(
                "DELETE FROM processed_events WHERE group_id = ? AND event_id = ? AND status = 'IN_PROGRESS'")) {
            delete.setString(1, groupId);
            delete.setString(2, eventId);
            delete.executeUpdate();
        }
    }

    public String statusOf(String eventId) throws Exception {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT status FROM processed_events WHERE group_id = ? AND event_id = ?")) {
            select.setString(1, groupId);
            select.setString(2, eventId);
            try (ResultSet rs = select.executeQuery()) {
                return rs.next() ? rs.getString("status") : null;
            }
        }
    }
}
