//
//  MLSMessageOrderingCoordinator.swift
//  Catbird
//
//  Created by Claude Code on 2024-12-26.
//
//  Coordinates message ordering across processes to ensure messages are processed
//  in sequence order. This prevents orphaned reactions and other race conditions
//  between NSE (Notification Service Extension) and the main app.
//
//  Thread-safe actor that uses shared SQLite database for cross-process state.
//

import Foundation
import OSLog
import CatbirdMLSCore
import Petrel

/// Decision result for whether a message can be processed now
public enum MessageOrderingDecision {
  case processNow           // seq == lastProcessed + 1, process immediately
  case buffer               // seq > lastProcessed + 1, buffer and wait
  case bufferForFutureEpoch // message epoch > local epoch, need to fetch commits first
  case alreadyProcessed     // seq <= lastProcessed, skip (duplicate)
  case forceProcess         // buffer timeout exceeded, process anyway
}

/// Coordinates message ordering across processes
/// Thread-safe, uses database for cross-process state
public actor MLSMessageOrderingCoordinator {

  // MARK: - Dependencies

  private let storage: MLSStorage
  private let logger: Logger

  // MARK: - Configuration

  /// Maximum time to wait for missing predecessor (seconds)
  public let maxWaitTime: TimeInterval = 5.0

  /// Maximum messages to buffer per conversation before force-flush
  public let maxBufferSize: Int = 50

  /// Maximum age of pending messages before cleanup (seconds)
  public let pendingMessageTimeout: TimeInterval = 300.0  // 5 minutes

  // MARK: - Initialization

  public init(storage: MLSStorage = .shared) {
    self.storage = storage
    self.logger = Logger(subsystem: "blue.catbird.mls", category: "MessageOrdering")
  }

  // MARK: - Public Interface

  /// Check if a message can be processed now or needs buffering
  /// Returns: .processNow, .buffer, .bufferForFutureEpoch, .alreadyProcessed, or .forceProcess
  ///
  /// - Parameters:
  ///   - messageID: Unique identifier for the message
  ///   - conversationID: Conversation/group ID
  ///   - sequenceNumber: Message sequence number
  ///   - messageEpoch: Optional epoch of the incoming message (from server)
  ///   - localEpoch: Optional current local epoch (from FFI/state)
  ///   - currentUserDID: Current user's DID
  ///   - database: Database for persistence
  /// - Returns: Decision on whether to process, buffer, or skip
  public func shouldProcessMessage(
    messageID: String,
    conversationID: String,
    sequenceNumber: Int64,
    messageEpoch: Int64? = nil,
    localEpoch: Int64? = nil,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MessageOrderingDecision {

    // FIX A: EPOCH-AWARE ORDERING
    // If we have epoch info and message is from a future epoch, buffer it.
    // This prevents DecryptionFailed errors from messages that arrive before
    // we've processed the commit that advances our epoch.
    if let msgEpoch = messageEpoch, let locEpoch = localEpoch {
      if msgEpoch > locEpoch {
        logger.info("[SEQ-ORDER] Message \(messageID.prefix(16)) epoch=\(msgEpoch) > local=\(locEpoch) - buffering for future epoch")
        return .bufferForFutureEpoch
      }
    }

    // 1. Get the last processed sequence number for this conversation
    let lastProcessedSeq = try await storage.getLastProcessedSeq(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: database
    )

    logger.debug("[SEQ-ORDER] Checking message \(messageID.prefix(16))... seq=\(sequenceNumber), lastProcessed=\(lastProcessedSeq)")

    // 2. Check if already processed (duplicate detection)
    // 2. Check if already processed (duplicate detection)
    if sequenceNumber <= lastProcessedSeq {
      // CRITICAL FIX: Even if seq <= lastProcessed, the message might be missing
      // (e.g. if we gap-jumped or force-processed without saving).
      // Check if it actually exists in DB before skipping.
      let exists = try await storage.fetchMessage(
        messageID: messageID,
        currentUserDID: currentUserDID,
        database: database
      ) != nil

      if exists {
        logger.info("[SEQ-ORDER] Message \(messageID.prefix(16)) seq=\(sequenceNumber) already processed & exists - skipping")
        return .alreadyProcessed
      } else {
        logger.warning("[SEQ-ORDER] Message \(messageID.prefix(16)) seq=\(sequenceNumber) is <= lastProcessed (\(lastProcessedSeq)) but MISSING from DB - re-processing")
        return .processNow
      }
    }

    // 3. Check if this is the next expected message in sequence
    let expectedSeq = lastProcessedSeq + 1
    if sequenceNumber == expectedSeq {
      logger.debug("[SEQ-ORDER] Message \(messageID.prefix(16)) seq=\(sequenceNumber) is next in sequence - processing now")
      return .processNow
    }

    // 4. Message arrived out of order - check if we should buffer or force-process
    if sequenceNumber > expectedSeq {
      // Check if this message is already pending
      let alreadyPending = try await storage.isMessagePending(
        messageID: messageID,
        currentUserDID: currentUserDID,
        database: database
      )

      if alreadyPending {
        logger.debug("[SEQ-ORDER] Message \(messageID.prefix(16)) seq=\(sequenceNumber) already buffered - skipping")
        return .alreadyProcessed
      }

      // Check buffer size - force flush if too many pending messages
      let pendingCount = try await storage.getAllPendingMessages(
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        database: database
      ).count

      if pendingCount >= maxBufferSize {
        logger.warning("[SEQ-ORDER] Buffer full (\(pendingCount) messages) for conversation \(conversationID.prefix(16)) - forcing flush")
        return .forceProcess
      }

      // Check if oldest pending message has exceeded timeout
      let allPending = try await storage.getAllPendingMessages(
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        database: database
      )

      if let oldest = allPending.first,
         Date().timeIntervalSince(oldest.receivedAt) > maxWaitTime {
          logger.warning("[SEQ-ORDER] Oldest pending message exceeded timeout (\(self.maxWaitTime)s) - forcing flush")
        return .forceProcess
      }

      logger.info("[SEQ-ORDER] Message \(messageID.prefix(16)) seq=\(sequenceNumber) out of order (expected \(expectedSeq)) - buffering")
      return .buffer
    }

    // Should never reach here, but default to processing
    logger.warning("[SEQ-ORDER] Unexpected sequence state for message \(messageID.prefix(16)) seq=\(sequenceNumber) - processing anyway")
    return .processNow
  }

  /// Record that a message was successfully processed
  /// Also triggers processing of any buffered messages that are now ready
  /// Returns: Array of buffered messages that are now ready to process
  public func recordMessageProcessed(
    messageID: String,
    conversationID: String,
    sequenceNumber: Int64,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSPendingMessageModel] {

    logger.debug("[SEQ-ORDER] Recording processed message \(messageID.prefix(16)) seq=\(sequenceNumber)")

    // 1. Update the last processed sequence number
    try await storage.updateLastProcessedSeq(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      sequenceNumber: sequenceNumber,
      database: database
    )

    // 2. Remove this message from pending buffer if it was there
    try? await storage.removePendingMessage(
      messageID: messageID,
      currentUserDID: currentUserDID,
      database: database
    )

    // 3. Find any buffered messages that are now ready to process
    // Get messages with seq == sequenceNumber + 1, sequenceNumber + 2, etc.
    // that form a contiguous sequence
    var readyMessages: [MLSPendingMessageModel] = []
    var nextExpectedSeq = sequenceNumber + 1

    while true {
      // Check if there's a pending message with the next sequence number
      guard let nextMessage = try await storage.getNextPendingMessage(
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        afterSeq: nextExpectedSeq - 1,
        database: database
      ) else {
        // No more contiguous messages in buffer
        break
      }

      // Only include if it's the exact next sequence (maintain ordering)
      if nextMessage.sequenceNumber == nextExpectedSeq {
        readyMessages.append(nextMessage)
        nextExpectedSeq = nextMessage.sequenceNumber + 1
        logger.debug("[SEQ-ORDER] Found ready buffered message \(nextMessage.messageID.prefix(16)) seq=\(nextMessage.sequenceNumber)")
      } else {
        // Gap in sequence, stop here
        break
      }
    }

    if !readyMessages.isEmpty {
      logger.info("[SEQ-ORDER] Returning \(readyMessages.count) ready buffered messages after processing seq=\(sequenceNumber)")
    }

    return readyMessages
  }

  /// Buffer a message for later processing
  public func bufferMessage(
    message: BlueCatbirdMlsDefs.MessageView,
    currentUserDID: String,
    source: String,  // "sse", "nse", "sync"
    database: MLSDatabase
  ) async throws {

    logger.info("[SEQ-ORDER] Buffering message \(message.id.prefix(16)) seq=\(message.seq) from \(source)")

    // 1. Serialize the MessageView to JSON
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601

    let messageViewJSON: Data
    do {
      messageViewJSON = try encoder.encode(message)
    } catch {
      logger.error("[SEQ-ORDER] Failed to serialize MessageView \(message.id.prefix(16)): \(error.localizedDescription)")
      throw error
    }

    // 2. Create pending message model
    let pending = MLSPendingMessageModel(
      messageID: message.id,
      currentUserDID: currentUserDID,
      conversationID: message.convoId,
      sequenceNumber: Int64(message.seq),
      epoch: Int64(message.epoch),
      messageViewJSON: messageViewJSON,
      receivedAt: Date(),
      processAttempts: 0,
      source: source
    )

    // 3. Store in database
    try await storage.bufferPendingMessage(pending, database: database)

    logger.debug("[SEQ-ORDER] Successfully buffered message \(message.id.prefix(16)) seq=\(message.seq) (\(messageViewJSON.count) bytes)")
  }

  /// Force-flush all buffered messages for a conversation (timeout scenario)
  /// Returns all pending messages in sequence order for processing
  public func flushBufferedMessages(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSPendingMessageModel] {

    logger.info("[SEQ-ORDER] Force-flushing all buffered messages for conversation \(conversationID.prefix(16))")

    // Get all pending messages for this conversation, sorted by sequence
    let allPending = try await storage.getAllPendingMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: database
    )

    if !allPending.isEmpty {
      logger.warning("[SEQ-ORDER] Flushing \(allPending.count) buffered messages for conversation \(conversationID.prefix(16))")

      // Log sequence numbers for debugging
      let seqs = allPending.map { $0.sequenceNumber }
      logger.debug("[SEQ-ORDER] Flushed sequences: \(seqs)")
    }

    return allPending
  }

  /// Cleanup old pending messages that exceeded the timeout
  /// Returns the number of messages cleaned up
  public func cleanupStaleMessages(
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Int {

      logger.debug("[SEQ-ORDER] Running cleanup for stale pending messages (timeout: \(self.pendingMessageTimeout)s)")

    let count = try await storage.cleanupOldPendingMessages(
      olderThan: pendingMessageTimeout,
      currentUserDID: currentUserDID,
      database: database
    )

    if count > 0 {
        logger.warning("[SEQ-ORDER] Cleaned up \(count) stale pending messages older than \(self.pendingMessageTimeout)s")
    } else {
      logger.debug("[SEQ-ORDER] No stale pending messages to clean up")
    }

    return count
  }
}
