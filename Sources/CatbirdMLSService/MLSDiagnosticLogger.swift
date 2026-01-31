//
//  MLSDiagnosticLogger.swift
//  Catbird
//
//  Centralized diagnostic logging for MLS operations.
//  Provides structured logging for debugging epoch desync, lock contention,
//  and orphaned reaction issues.
//

import Foundation
import OSLog

/// Centralized diagnostic logger for MLS operations
/// Provides structured logging for key events to aid debugging
public final class MLSDiagnosticLogger: Sendable {
  
  public static let shared = MLSDiagnosticLogger()
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "Diagnostics")
  
  private init() {}
  
  // MARK: - State Tracking Events
  
  /// Log an epoch transition
  /// - Parameters:
  ///   - convoId: Conversation ID
  ///   - oldEpoch: Previous epoch value
  ///   - newEpoch: New epoch value
  ///   - trigger: What caused the transition (e.g., "commit", "proposal", "sync")
  public func logEpochTransition(convoId: String, oldEpoch: UInt64, newEpoch: UInt64, trigger: String) {
    logger.info("[STATE-TRACK] Epoch advanced: \(oldEpoch) -> \(newEpoch) for \(convoId.prefix(20)) (trigger: \(trigger))")
  }
  
  /// Log when a self-sent message echo is detected
  /// - Parameters:
  ///   - messageId: Message ID
  ///   - msgEpoch: Epoch in the message
  ///   - localEpoch: Current local epoch
  public func logSelfEchoDetected(messageId: String, msgEpoch: Int64, localEpoch: UInt64) {
    logger.info("[SELF-ECHO] Detected self-sent message \(messageId) from epoch \(msgEpoch) (local: \(localEpoch))")
  }
  
  /// Log a decryption attempt outcome
  /// - Parameters:
  ///   - messageId: Message ID
  ///   - sender: Sender DID
  ///   - epoch: Message epoch
  ///   - outcome: Result (e.g., "success", "epoch_skip", "decryption_error")
  public func logDecryptionAttempt(messageId: String, sender: String, epoch: Int64, outcome: String) {
    logger.debug("[DECRYPT] Message \(messageId) from \(sender.prefix(20)) epoch=\(epoch): \(outcome)")
  }
  
  // MARK: - Lock Events
  
  /// Log when a lock is acquired
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - holder: Component/operation holding the lock
  ///   - waitDuration: Time spent waiting for the lock (seconds)
  public func logLockAcquired(userDID: String, holder: String, waitDuration: TimeInterval) {
    let durationStr = String(format: "%.2f", waitDuration)
    logger.info("[LOCK] Acquired for \(userDID.prefix(20), privacy: .private) by \(holder) (wait: \(durationStr)s)")
  }
  
  /// Log lock contention
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - waiter: Component/operation waiting for the lock
  ///   - currentHolder: Component/operation currently holding the lock (if known)
  public func logLockContention(userDID: String, waiter: String, currentHolder: String? = nil) {
    if let holder = currentHolder {
      logger.warning("[LOCK-CONTENTION] \(waiter) waiting for lock held by \(holder) (user: \(userDID.prefix(20), privacy: .private))")
    } else {
      logger.warning("[LOCK-CONTENTION] \(waiter) waiting for lock (user: \(userDID.prefix(20), privacy: .private))")
    }
  }
  
  /// Log when a lock is released
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - heldDuration: How long the lock was held (seconds)
  public func logLockReleased(userDID: String, heldDuration: TimeInterval) {
    let durationStr = String(format: "%.2f", heldDuration)
    if heldDuration > 10.0 {
      logger.warning("[LOCK] Released for \(userDID.prefix(20), privacy: .private) (held: \(durationStr)s - LONG)")
    } else {
      logger.debug("[LOCK] Released for \(userDID.prefix(20), privacy: .private) (held: \(durationStr)s)")
    }
  }
  
  /// Log potential lock deadlock warning
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - holder: Component holding the lock
  ///   - duration: How long the lock has been held
  public func logLockWatchdogWarning(userDID: String, holder: String, duration: TimeInterval) {
    let durationStr = String(format: "%.1f", duration)
    logger.error("[LOCK-WATCHDOG] Lock held for \(durationStr)s by \(holder) - possible deadlock (user: \(userDID.prefix(20), privacy: .private))")
  }
  
  // MARK: - Orphan Events
  
  /// Log when a reaction is orphaned (parent message not found)
  /// - Parameters:
  ///   - reactionId: Reaction ID
  ///   - parentMessageId: Missing parent message ID
  ///   - emoji: The reaction emoji
  ///   - conversationId: Conversation ID
  public func logOrphanCreated(reactionId: String, parentMessageId: String, emoji: String, conversationId: String) {
    logger.info("[ORPHAN] Created orphaned reaction \(emoji) (id: \(reactionId)) for missing message \(parentMessageId) in \(conversationId.prefix(20))")
  }
  
  /// Log when orphaned reactions are adopted after parent message arrives
  /// - Parameters:
  ///   - count: Number of reactions adopted
  ///   - parentMessageId: The parent message ID
  public func logOrphansAdopted(count: Int, parentMessageId: String) {
    logger.info("[ORPHAN-ADOPT] Adopted \(count) orphaned reaction(s) for message \(parentMessageId)")
  }
  
  /// Log when a parent message fetch is triggered for orphans
  /// - Parameters:
  ///   - messageId: The missing parent message ID
  ///   - conversationId: Conversation ID
  public func logOrphanParentFetchTriggered(messageId: String, conversationId: String) {
    logger.info("[ORPHAN-FETCH] Triggering fetch for missing parent message \(messageId) in \(conversationId.prefix(20))")
  }
  
  // MARK: - Sync Events
  
  /// Log a sync operation start
  /// - Parameters:
  ///   - conversationId: Conversation ID (nil for full sync)
  ///   - reason: Reason for the sync
  public func logSyncStarted(conversationId: String?, reason: String) {
    if let convoId = conversationId {
      logger.info("[SYNC] Starting sync for \(convoId.prefix(20)): \(reason)")
    } else {
      logger.info("[SYNC] Starting full sync: \(reason)")
    }
  }
  
  /// Log a sync operation completion
  /// - Parameters:
  ///   - conversationId: Conversation ID (nil for full sync)
  ///   - messagesProcessed: Number of messages processed
  ///   - duration: Time taken (seconds)
  public func logSyncCompleted(conversationId: String?, messagesProcessed: Int, duration: TimeInterval) {
    let durationStr = String(format: "%.2f", duration)
    if let convoId = conversationId {
      logger.info("[SYNC] Completed for \(convoId.prefix(20)): \(messagesProcessed) messages in \(durationStr)s")
    } else {
      logger.info("[SYNC] Full sync completed: \(messagesProcessed) messages in \(durationStr)s")
    }
  }
  
  /// Log a sync failure
  /// - Parameters:
  ///   - conversationId: Conversation ID (nil for full sync)
  ///   - error: The error that occurred
  public func logSyncFailed(conversationId: String?, error: Error) {
    if let convoId = conversationId {
      logger.error("[SYNC] Failed for \(convoId.prefix(20)): \(error.localizedDescription)")
    } else {
      logger.error("[SYNC] Full sync failed: \(error.localizedDescription)")
    }
  }
  
  // MARK: - E2E Test Support
  
  /// Log E2E mode startup marker
  /// - Parameter runId: Unique E2E run identifier
  public func logE2EModeStarted(runId: String) {
    logger.info("[E2E] MLS E2E mode started, run_id=\(runId)")
  }
  
  /// Log MLS ready state for E2E detection
  /// - Parameter userDID: Authenticated user's DID
  public func logMLSReady(userDID: String) {
    logger.info("[E2E] MLS ready for user=\(userDID.prefix(20), privacy: .private)")
  }
  
  /// Log MLS state dump for E2E diagnostics
  /// - Parameters:
  ///   - conversationCount: Number of active conversations
  ///   - groupCount: Number of MLS groups
  ///   - pendingMessages: Number of pending outbound messages
  public func logMLSStateDump(conversationCount: Int, groupCount: Int, pendingMessages: Int) {
    logger.info("[E2E] MLS state: conversations=\(conversationCount), groups=\(groupCount), pending=\(pendingMessages)")
  }
  
  /// Log message send for E2E tracking
  /// - Parameters:
  ///   - correlationId: Message correlation ID
  ///   - conversationId: Target conversation
  ///   - contentPreview: First 20 chars of content (for test matching)
  public func logE2EMessageSent(correlationId: String, conversationId: String, contentPreview: String) {
    logger.info("[E2E-SEND] correlation_id=\(correlationId), convo=\(conversationId.prefix(20)), content=\(contentPreview.prefix(20))")
  }
  
  /// Log message receive for E2E tracking
  /// - Parameters:
  ///   - messageId: Received message ID
  ///   - correlationId: Correlation ID if available
  ///   - conversationId: Source conversation
  ///   - sender: Sender DID
  ///   - contentPreview: First 20 chars of content (for test matching)
  public func logE2EMessageReceived(messageId: String, correlationId: String?, conversationId: String, sender: String, contentPreview: String) {
    let corrId = correlationId ?? "none"
    logger.info("[E2E-RECV] msg_id=\(messageId), correlation_id=\(corrId), convo=\(conversationId.prefix(20)), sender=\(sender.prefix(20)), content=\(contentPreview.prefix(20))")
  }
  
  /// Log delivery acknowledgement for E2E tracking
  /// - Parameters:
  ///   - messageId: Acknowledged message ID
  ///   - latencyMs: Delivery latency in milliseconds
  public func logE2EDeliveryAck(messageId: String, latencyMs: Int) {
    logger.info("[E2E-ACK] msg_id=\(messageId), latency_ms=\(latencyMs)")
  }
}
