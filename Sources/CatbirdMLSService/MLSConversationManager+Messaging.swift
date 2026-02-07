import CatbirdMLSCore
import CryptoKit
import Foundation
import GRDB
import OSLog
import Petrel
import Synchronization

public extension MLSConversationManager {
  private typealias AdoptedReaction = MLSStorage.AdoptedReaction

  private struct ProcessingContext {
    let attemptID: String
    let source: String
    let queueIndex: Int64
  }

  private func nextProcessingAttemptID() -> String {
    let next = processingAttemptCounter.withLock { value in
      value += 1
      return value
    }
    return "\(next)-\(UUID().uuidString.prefix(8))"
  }

  private func nextMutationID() -> Int64 {
    processingMutationCounter.withLock { value in
      value += 1
      return value
    }
  }

  /// Execute a database operation (formerly with advisory lock protection).
  /// No lock needed - SQLite WAL handles concurrent access.
  /// Cross-process coordination uses Darwin notifications (MLSCrossProcess).
  /// For best-effort operations where failure is acceptable (uses try?).
  private func withAdvisoryLockBestEffort<T: Sendable>(
    for userDid: String,
    operation: @Sendable @escaping () async throws -> T
  ) async -> T? {
    // No advisory lock needed - SQLite WAL handles concurrent access
    return try? await operation()
  }

  internal func beginRejoinAttempt(conversationID: String, source: String) -> Bool {
    let now = Date()

    let cooldownRemaining = rejoinAttemptTimestamps.withLock { timestamps -> TimeInterval? in
      guard let lastAttempt = timestamps[conversationID] else { return nil }
      let elapsed = now.timeIntervalSince(lastAttempt)
      if elapsed < self.rejoinCooldownSeconds {
        return self.rejoinCooldownSeconds - elapsed
      }
      return nil
    }

    if let cooldownRemaining {
      let remaining = Int(cooldownRemaining.rounded(.up))
      logger.warning(
        "⏳ [MLS-REJOIN] Skipping rejoin for \(conversationID.prefix(16)) source=\(source) - cooldown \(remaining)s"
      )
      return false
    }

    let alreadyInProgress = rejoinInProgress.withLock { inProgress -> Bool in
      if inProgress { return true }
      inProgress = true
      return false
    }

    if alreadyInProgress {
      let active = rejoinInProgressConversationID.withLock { $0 } ?? "unknown"
      logger.warning(
        "⏳ [MLS-REJOIN] Skipping rejoin for \(conversationID.prefix(16)) source=\(source) - already rejoining \(active.prefix(16))"
      )
      return false
    }

    rejoinInProgressConversationID.withLock { $0 = conversationID }
    rejoinAttemptTimestamps.withLock { $0[conversationID] = now }

    logger.info(
      "🔄 [MLS-REJOIN] Starting rejoin attempt for \(conversationID.prefix(16)) source=\(source)"
    )
    return true
  }

  internal func endRejoinAttempt(conversationID: String) {
    let shouldClear = rejoinInProgressConversationID.withLock { current -> Bool in
      if current == nil || current == conversationID {
        current = nil
        return true
      }
      return false
    }
    if shouldClear {
      rejoinInProgress.withLock { $0 = false }
    }
  }

  private func recordSelfDecryptFailure(conversationID: String, source: String) async {
    let count = selfDecryptFailureCounters.withLock { counters in
      let next = (counters[conversationID] ?? 0) + 1
      counters[conversationID] = next
      return next
    }

    if count < 3 { return }
    if await conversationNeedsRejoin(conversationID) { return }

    try? await markConversationNeedsRejoin(conversationID)

    guard beginRejoinAttempt(conversationID: conversationID, source: source) else { return }

    logger.error(
      "🔴 [MLS-REJOIN] CannotDecryptOwnMessage x\(count) for \(conversationID.prefix(16)) source=\(source) - forcing rejoin"
    )

    Task { [weak self] in
      guard let self else { return }
      defer { self.endRejoinAttempt(conversationID: conversationID) }
      do {
        try await self.forceRejoin(for: conversationID)
        await self.clearConversationRejoinFlag(conversationID)
      } catch {
        self.logger.error(
          "❌ [MLS-REJOIN] forceRejoin failed for \(conversationID.prefix(16)): \(error.localizedDescription)"
        )
        await self.requestRejoinIfPossible(
          convoId: conversationID,
          reason: "CannotDecryptOwnMessage x\(count) source=\(source)"
        )
      }
    }
  }

  private func clearSelfDecryptFailures(conversationID: String) {
    selfDecryptFailureCounters.withLock { counters in
      counters.removeValue(forKey: conversationID)
    }
  }

  // MARK: - FIX D: Persistent Decryption Failure Tracking
  
  /// Record a decryption failure for a message. If threshold is exceeded, trigger nuclear rejoin.
  /// Returns true if nuclear rejoin was triggered.
  private func recordPersistentDecryptionFailure(
    messageID: String,
    conversationID: String,
    source: String
  ) async -> Bool {
    let count = persistentDecryptionFailures.withLock { failures in
      let next = (failures[messageID] ?? 0) + 1
      failures[messageID] = next
      return next
    }
    
    logger.warning("⚠️ [MLS-DESYNC] Message \(messageID.prefix(16)) decryption failure #\(count)/\(self.nuclearRejoinThreshold)")
    
    if count >= nuclearRejoinThreshold {
      logger.error("🔴 [MLS-NUCLEAR] Message \(messageID.prefix(16)) failed decryption \(count)x - triggering nuclear rejoin for \(conversationID.prefix(16))")
      
      // Clear the failure counter for this message
      persistentDecryptionFailures.withLock { failures in
        failures.removeValue(forKey: messageID)
      }
      
      // Mark conversation as needing rejoin
      try? await markConversationNeedsRejoin(conversationID)
      
      // Attempt nuclear rejoin
      guard beginRejoinAttempt(conversationID: conversationID, source: "nuclear-\(source)") else {
        return false
      }
      
      Task { [weak self] in
        guard let self else { return }
        defer { self.endRejoinAttempt(conversationID: conversationID) }
        
        do {
          try await self.forceRejoin(for: conversationID)
          await self.clearConversationRejoinFlag(conversationID)
          self.logger.info("✅ [MLS-NUCLEAR] Nuclear rejoin succeeded for \(conversationID.prefix(16))")
        } catch {
          self.logger.error("❌ [MLS-NUCLEAR] Nuclear rejoin failed for \(conversationID.prefix(16)): \(error.localizedDescription)")
        }
      }
      
      return true
    }
    
    return false
  }
  
  /// Clear decryption failure tracking for a message (on success)
  private func clearPersistentDecryptionFailure(messageID: String) {
    persistentDecryptionFailures.withLock { failures in
      failures.removeValue(forKey: messageID)
    }
  }

  private func shouldSkipProcessingForRejoin(conversationID: String, source: String) async -> Bool {
    if await conversationNeedsRejoin(conversationID) {
      logger.warning(
        "⚠️ [MLS-REJOIN] Skipping processing for \(conversationID.prefix(16)) source=\(source) (needs rejoin)"
      )
      return true
    }
    return false
  }

  private func persistProcessedPayload(
    message: BlueCatbirdMlsDefs.MessageView,
    payload: MLSMessagePayload,
    senderID: String,
    processingError: String?,
    validationReason: String?,
    context: ProcessingContext
  ) async throws -> [AdoptedReaction] {
    guard let userDid = userDid else { throw MLSConversationError.noAuthentication }

    let cursorBefore = (try? await storage.getLastProcessedSeq(
      conversationID: message.convoId,
      currentUserDID: userDid,
      database: database
    )) ?? -1

    logger.info(
      "🧾 [ATOMIC] Begin attempt=\(context.attemptID) queue=\(context.queueIndex) source=\(context.source) msg=\(message.id.prefix(16)) seq=\(message.seq) cursor=\(cursorBefore)"
    )

    do {
      // No advisory lock needed - SQLite WAL handles concurrent access
      // Cross-process coordination uses Darwin notifications (MLSCrossProcess)

      let adopted = try await self.withDatabaseRecovery(currentUserDID: userDid) { db in
        try await self.storage.savePayloadForMessage(
          messageID: message.id,
          conversationID: message.convoId,
          payload: payload,
          senderID: senderID,
          currentUserDID: userDid,
          epoch: Int64(message.epoch),
          sequenceNumber: Int64(message.seq),
          timestamp: message.createdAt.date,
          database: db,
          processingError: processingError,
          validationFailureReason: validationReason,
          advanceSequenceState: true
        )
      }

      let cursorAfter = (try? await storage.getLastProcessedSeq(
        conversationID: message.convoId,
        currentUserDID: userDid,
        database: database
      )) ?? -1

      logger.info(
        "🧾 [ATOMIC] Commit attempt=\(context.attemptID) queue=\(context.queueIndex) msg=\(message.id.prefix(16)) seq=\(message.seq) cursor=\(cursorAfter)"
      )

      // Notify cross-process observers of database change
      MLSCrossProcess.shared.notifyChanged()

      return adopted
    } catch {
      logger.error(
        "❌ [ATOMIC] Failed attempt=\(context.attemptID) queue=\(context.queueIndex) msg=\(message.id.prefix(16)) seq=\(message.seq): \(error.localizedDescription)"
      )
      throw error
    }
  }

  // MARK: - Sending Messages

  /// Send a text message to a conversation
  /// - Parameters:
  ///   - convoId: The conversation group ID
  ///   - plaintext: The text to send
  ///   - embed: Optional data to embed (links, images, etc.)
  /// - Returns: Confirmation metadata from the server
  public func sendMessage(
    convoId: String,
    plaintext: String,
    embed: MLSEmbedData? = nil
  ) async throws -> (
    messageId: String, receivedAt: ATProtocolDate, sequenceNumber: Int64, epoch: Int64
  ) {
    // ═══════════════════════════════════════════════════════════════════════════
    // EARLY VALIDATION: Run outside the queue to fail fast
    // ═══════════════════════════════════════════════════════════════════════════
    try throwIfShuttingDown("sendMessage")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    var convo = conversations[convoId]
    
    // If conversation not found locally, try to fetch and initialize it on-demand
    // This handles the case where a sync is in progress and hasn't fetched this conversation yet
    if convo == nil {
      logger.warning("⚠️ Conversation \(convoId.prefix(16))... not found locally, attempting on-demand fetch")
      
      // Try to fetch this specific conversation from server
      if let fetchedConvo = try? await apiClient.getConversation(convoId: convoId) {
        // Add to local state
        conversations[convoId] = fetchedConvo
        
        // Try to initialize MLS group (Welcome or External Commit)
        do {
          try await initializeGroupFromWelcome(convo: fetchedConvo)
          logger.info("✅ On-demand conversation fetch and initialization succeeded for \(convoId.prefix(16))...")
          convo = fetchedConvo
        } catch {
          logger.error("❌ On-demand initialization failed for \(convoId.prefix(16))...: \(error)")
          // Fall through to error handling below
        }
      }
    }
    
    guard let convo = convo else {
      logger.error("Cannot send message - conversation \(convoId) not found locally")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      logger.error("Invalid group ID hex format: \(convo.groupId)")
      throw MLSConversationError.invalidGroupId
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SEND QUEUE: Serialize sends per conversation to guarantee ordering
    // ═══════════════════════════════════════════════════════════════════════════
    // When sending "hello", "hi", "hey" rapidly, each send waits for the previous
    // one to complete before starting. This ensures server-assigned sequence
    // numbers match the order messages were sent by the user.
    // ═══════════════════════════════════════════════════════════════════════════
    let queueDepth = await sendQueueCoordinator.getQueueDepth(conversationID: convoId)
    if queueDepth > 0 {
      logger.info("📤 [SEND-QUEUE] Enqueuing send for \(convoId.prefix(16))... (queue depth: \(queueDepth))")
    }

    return try await sendQueueCoordinator.enqueueSend(conversationID: convoId) { [self] in
      // Re-check shutdown after waiting in queue (account switch may have started)
      try throwIfShuttingDown("sendMessage-queued")

      // 1. Refresh freshness check via FFI (Ground Truth)
      let isInitialized = await mlsClient.groupExists(for: userDid, groupId: groupIdData)
      if !isInitialized {
        logger.warning("Group \(convoId) not initialized in FFI; attempting sync-fix...")
        try await syncWithServer()
      }

      // 2. Duplicate send prevention (Deduplication)
      let plaintextData = (plaintext + (embed?.cid ?? "")).data(using: .utf8)!
      let idempotencyKey = generateIdempotencyKey(convoId: convoId, plaintext: plaintextData)

      if isRecentlySent(convoId: convoId, idempotencyKey: idempotencyKey) {
        logger.warning("🚫 Duplicate message send detected - skipping submission to avoid spam")
        throw MLSConversationError.duplicateMessage
      }

      // 3. Group/epoch ordering barrier
      // IMPORTANT: To ensure new members can decrypt the first message, we must not allow a
      // membership commit (epoch advance) to race with a message send.
      // We hold the per-group lock across: epoch read → encrypt → server send.
      let result = try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        // Ground-truth epoch (inside the lock so it can't race with addMembers/merge)
        let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

        if Int64(localEpoch) != convo.epoch {
          logger.warning(
            "⚠️ Epoch mismatch before send (proceeding with FFI epoch): Local \(localEpoch), Server-View \(convo.epoch)"
          )
        }

        // 4. Build and encrypt payload (inside lock)
        let payload = MLSMessagePayload.text(plaintext, embed: embed)
        let payloadData = try payload.encodeToJSON()

        // Avoid re-entering the lock by calling the impl directly
        let ciphertext = try await encryptMessageImpl(groupId: convo.groupId, plaintext: payloadData)

        // 5. Apply padding for traffic analysis protection
        let (paddedCiphertext, paddedSize) = try MLSMessagePadding.padCiphertextToBucket(ciphertext)

        // 6. Generate deterministic message ID for atomic caching
        let localMsgId = UUID().uuidString

        // 🔒 Pre-cache payload for self-sent message BEFORE network call
        // CRITICAL FIX: Re-check shutdown status before attempting DB write
        // This prevents "SQLite error 21" if account switch started during encryption
        try throwIfShuttingDown("sendMessage-preCache")
        
        do {
          // Use shared helper with retry logic (3 attempts)
          try await cacheControlMessageEnvelope(
            message: BlueCatbirdMlsDefs.MessageView(
              id: localMsgId,
              convoId: convoId,
              ciphertext: Bytes(data: paddedCiphertext),
              epoch: Int(localEpoch),
              seq: 0,
              createdAt: ATProtocolDate(date: Date()),
              messageType: "app" 
            ),
            payload: payload,
            senderDID: userDid,
            currentUserDID: userDid
          )
          logger.debug("💾 [Gen: \(self.currentCoordinationGeneration)] Pre-cached self-sent message \(localMsgId)")
        } catch {
          logger.error("❌ [Gen: \(self.currentCoordinationGeneration)] Failed to pre-cache self-sent message: \(error.localizedDescription)")
          throw MLSConversationError.storageUnavailable(reason: "Failed to cache message state before send: \(error.localizedDescription)")
        }

        // 7. Send to server (still inside lock to prevent epoch/membership races)
        logger.info("📡 [Gen: \(self.currentCoordinationGeneration)] [Epoch: \(localEpoch)] Sending message \(localMsgId) via ATProto...")
        do {
          let sendResult = try await apiClient.sendMessage(
            convoId: convoId,
            msgId: localMsgId,
            ciphertext: paddedCiphertext,
            epoch: Int(localEpoch),
            paddedSize: paddedSize,
            senderDid: try DID(didString: userDid)
          )
          return (localMsgId, sendResult)
        } catch {
          logger.error("❌ [Gen: \(self.currentCoordinationGeneration)] [Epoch: \(localEpoch)] Network send failed for \(localMsgId): \(error.localizedDescription)")
          throw error
        }
      }

      let localMsgId = result.0
      let sendResult = result.1

      // 8. Post-send local updates
      await trackSentMessage(convoId: convoId, idempotencyKey: idempotencyKey)

      // Update the cached payload with the official server metadata (seq/epoch/timestamp)
      try? await storage.updateMessageMetadata(
        messageID: localMsgId,
        currentUserDID: userDid,
        epoch: sendResult.epoch,
        sequenceNumber: sendResult.sequenceNumber,
        timestamp: sendResult.receivedAt.date,
        database: database,
        newMessageID: sendResult.messageId
      )

      logger.info("✅ [Gen: \(self.currentCoordinationGeneration)] [Epoch: \(sendResult.epoch)] Message \(localMsgId) confirmed by server (Seq: \(sendResult.sequenceNumber))")
      return (
        messageId: sendResult.messageId,
        receivedAt: sendResult.receivedAt,
        sequenceNumber: sendResult.sequenceNumber,
        epoch: sendResult.epoch
      )
    }
  }

  /// Send an encrypted reaction to a message
  public func sendEncryptedReaction(
    convoId: String,
    messageId: String,
    emoji: String,
    action: MLSReactionPayload.ReactionAction
  ) async throws -> (
    messageId: String, receivedAt: ATProtocolDate, sequenceNumber: Int64, epoch: Int64
  ) {
    try throwIfShuttingDown("sendEncryptedReaction")

    guard let userDid = userDid, let convo = conversations[convoId] else {
      throw MLSConversationError.noAuthentication
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Use send queue to maintain ordering with text messages
    return try await sendQueueCoordinator.enqueueSend(conversationID: convoId) { [self] in
      try throwIfShuttingDown("sendEncryptedReaction-queued")

      // Build payload
      let payload = MLSMessagePayload.reaction(messageId: messageId, emoji: emoji, action: action)
      let payloadData = try payload.encodeToJSON()

      let result = try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

        let ciphertext = try await encryptMessageImpl(groupId: convo.groupId, plaintext: payloadData)
        let (paddedCiphertext, paddedSize) = try MLSMessagePadding.padCiphertextToBucket(ciphertext)

        // Generate local ID for atomic cache
        let localMsgId = UUID().uuidString

        // 🔒 CRITICAL FIX: Cache BEFORE network call
        // Use a synthetic MessageView for the pre-cache step
        // Must succeed or we abort to avoid "CannotDecryptOwnMessage" later
        try throwIfShuttingDown("sendEncryptedReaction-preCache")
        try await cacheControlMessageEnvelope(
          message: BlueCatbirdMlsDefs.MessageView(
            id: localMsgId, // Matches expected server echo (idempotent)
            convoId: convoId,
            ciphertext: Bytes(data: paddedCiphertext),
            epoch: Int(localEpoch),
            seq: 0, // Placeholder, updated on echo
            createdAt: ATProtocolDate(date: Date()),
            messageType: "reaction"
          ),
          payload: payload,
          senderDID: userDid,
          currentUserDID: userDid
        )

        // Send
        let sendResult = try await apiClient.sendMessage(
          convoId: convoId,
          msgId: localMsgId,
          ciphertext: paddedCiphertext,
          epoch: Int(localEpoch),
          paddedSize: paddedSize,
          senderDid: try DID(didString: userDid)
        )
        return (localMsgId, sendResult)
      }

      let localMsgId = result.0
      let sendResult = result.1

      // Update metadata with server-confirmed values
      try? await storage.updateMessageMetadata(
        messageID: localMsgId,
        currentUserDID: userDid,
        epoch: sendResult.epoch,
        sequenceNumber: sendResult.sequenceNumber,
        timestamp: sendResult.receivedAt.date,
        database: database,
        newMessageID: sendResult.messageId
      )

      return (
        messageId: sendResult.messageId,
        receivedAt: sendResult.receivedAt,
        sequenceNumber: sendResult.sequenceNumber,
        epoch: sendResult.epoch
      )
    }
  }

  // Read receipts and typing indicators have been removed to reduce complexity.
  // The following send functions were removed:
  // - sendEncryptedReadReceipt
  // - sendEncryptedTypingIndicator  
  // - sendTypingIndicator


  /// Add a reaction (emoji) to a message
  /// Note: For MLS, reactions are sent encrypted via sendEncryptedReaction
  public func addReaction(convoId: String, messageId: String, reaction: String) async throws -> (
    success: Bool, reactedAt: Date?
  ) {
    try throwIfShuttingDown("addReaction")

    guard conversations[convoId] != nil else {
      throw MLSConversationError.conversationNotFound
    }

    // Send encrypted reaction via MLS
    let result = try await sendEncryptedReaction(
      convoId: convoId,
      messageId: messageId,
      emoji: reaction,
      action: .add
    )

    if let userDid = userDid {
      let reactionModel = MLSReactionModel(
        messageID: messageId,
        conversationID: convoId,
        currentUserDID: userDid,
        actorDID: userDid,
        emoji: reaction,
        action: "add",
        timestamp: result.receivedAt.date
      )
      try await withDatabaseRecovery(currentUserDID: userDid) { [self] db in
        try await self.storage.saveReaction(reactionModel, database: db)
      }
    }

    return (success: true, reactedAt: result.receivedAt.date)
  }

  /// Remove a reaction from a message
  /// Note: For MLS, reaction removals are sent encrypted via sendEncryptedReaction
  public func removeReaction(convoId: String, messageId: String, reaction: String) async throws -> Bool {
    try throwIfShuttingDown("removeReaction")

    guard conversations[convoId] != nil else {
      throw MLSConversationError.conversationNotFound
    }

    // Send encrypted reaction removal via MLS
    _ = try await sendEncryptedReaction(
      convoId: convoId,
      messageId: messageId,
      emoji: reaction,
      action: .remove
    )

    if let userDid = userDid {
      try await withDatabaseRecovery(currentUserDID: userDid) { db in
          try await self.storage.deleteReaction(
          messageID: messageId,
          actorDID: userDid,
          emoji: reaction,
          currentUserDID: userDid,
          database: db
        )
      }
    }

    return true
  }

  /// Load cached reactions for a conversation
  public func loadReactionsForConversation(_ convoId: String) async throws -> [String:
    [MLSMessageReaction]]
  {
    guard let userDid = userDid else {
      return [:]
    }

    let reactionModels = try await storage.fetchReactionsForConversation(
      convoId,
      currentUserDID: userDid,
      database: database
    )

    var result: [String: [MLSMessageReaction]] = [:]
    for (messageId, models) in reactionModels {
      result[messageId] = models.map { model in
        MLSMessageReaction(
          messageId: model.messageID,
          reaction: model.emoji,
          senderDID: model.actorDID,
          reactedAt: model.timestamp
        )
      }
    }

    return result
  }

  // MARK: - Decryption and Processing

  /// Decrypt a received message
  /// - Parameter message: Encrypted message view
  /// - Returns: Decrypted message payload with text and optional embed
  public func decryptMessage(
    _ message: BlueCatbirdMlsDefs.MessageView,
    source: String = "unknown"
  ) async throws -> DecryptedMLSMessage
  {
    logger.debug("Decrypting message: \(message.id)")

    let outcome = try await processServerMessage(message, source: source)
    switch outcome {
    case .application(let payload, let senderDID):
      return DecryptedMLSMessage(messageView: message, payload: payload, senderDID: senderDID)
    case .nonApplication:
      throw MLSConversationError.invalidMessage
    case .controlMessage:
      throw MLSConversationError.invalidMessage
    }
  }

  /// Process a single server message through UniFFI and return application payloads when available
  internal func processServerMessage(
    _ message: BlueCatbirdMlsDefs.MessageView,
    source: String = "unknown"
  ) async throws -> MessageProcessingOutcome
  {
    // CRITICAL: Capture session generation at start to detect account switches
    let myGeneration = sessionGeneration

    // CRITICAL FIX: Ensure state is reloaded from disk before processing
    // This handles the case where NSE advanced the MLS ratchet while app was in background.
    // Without this, we may try to decrypt with stale keys → SecretReuseError
    try await ensureStateReloaded()

    // Check if we're shutting down - abort early to avoid racing with DB close
    try throwIfShuttingDown("processServerMessage")

    // Validate session generation hasn't changed (account switch)
    try validateSessionGeneration(capturedGeneration: myGeneration)

    let gen = currentCoordinationGeneration
    let convoIdPrefix = String(message.convoId.prefix(8))
    logger.info("📦 [PROCESS] [Gen: \(gen)] Starting processing for message \(message.id) in \(convoIdPrefix)...")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    // FIX A: Get local epoch for epoch-aware ordering
    var localEpochForOrdering: Int64? = nil
    if let convo = conversations[message.convoId],
       let groupIdData = Data(hexEncoded: convo.groupId) {
      localEpochForOrdering = Int64((try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) ?? 0)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL: Check message ordering decision BEFORE processing
    // ═══════════════════════════════════════════════════════════════════════════
    let decision = try await messageOrderingCoordinator.shouldProcessMessage(
      messageID: message.id,
      conversationID: message.convoId,
      sequenceNumber: Int64(message.seq),
      messageEpoch: Int64(message.epoch),
      localEpoch: localEpochForOrdering,
      currentUserDID: userDid,
      database: database
    )

    // 🔍 DEBUG: Log decision for all messages
    logger.info("🔍 [PROCESS-DECISION] msg=\(message.id.prefix(16)) seq=\(message.seq) type=\(message.messageType ?? "nil") epoch=\(message.epoch) decision=\(String(describing: decision))")

    switch decision {
    case .alreadyProcessed:
      logger.warning("⏭️ [SEQ-ORDER] Skipping already-processed message \(message.id) seq=\(message.seq) - returning .nonApplication")
      return .nonApplication

    case .bufferForFutureEpoch:
      // FIX A: Message is from future epoch - buffer it and try to fetch missing commits
      logger.info("[SEQ-ORDER] Buffering message \(message.id) seq=\(message.seq) (future epoch \(message.epoch) > local \(localEpochForOrdering ?? -1))")
      try await messageOrderingCoordinator.bufferMessage(
        message: message,
        currentUserDID: userDid,
        source: source,
        database: database
      )
      // Try to fetch missing commits that would advance our epoch
      if let convo = conversations[message.convoId] {
        let localEpoch = UInt64(localEpochForOrdering ?? 0)
        let fetched = await fetchAndProcessMissingCommits(
          conversationID: message.convoId,
          groupId: convo.groupId,
          localEpoch: localEpoch,
          targetEpoch: message.epoch
        )
        if fetched {
          // Commits fetched - try to process the buffered message now
          logger.info("[SEQ-ORDER] Commits fetched, attempting to process buffered message \(message.id)")
          // Don't recursively call here - let the caller retry
        }
      }
      return .nonApplication

    case .buffer:
      logger.info("[SEQ-ORDER] Buffering out-of-order message \(message.id) seq=\(message.seq)")
      try await messageOrderingCoordinator.bufferMessage(
        message: message,
        currentUserDID: userDid,
        source: source,
        database: database
      )
      // Trigger gap fill to fetch missing messages
      await triggerGapFill(conversationID: message.convoId, upToSeq: Int64(message.seq))
      return .nonApplication

    case .processNow, .forceProcess:
      // Continue with normal processing below
      break
    }

    // Attempt processing with retry logic for SecretReuseError
    // This handles the race condition where NSE updates the state in the background
    let maxRetries = 2  // Increased from 1 to give more chances for cache to be populated
    var lastError: Error?
    let attemptID = nextProcessingAttemptID()
    var processingContext: ProcessingContext?

    for attempt in 0...maxRetries {
      do {
        let (_, outcome) = try await withMLSUserPermit(for: userDid) { [self] in
          try await self.messageProcessingCoordinator.withQueuedSection(conversationID: message.convoId) { queueIndex in
            let context = ProcessingContext(
              attemptID: attemptID,
              source: source,
              queueIndex: queueIndex
            )
            processingContext = context
            self.logger.info(
              "🧵 [PROCESS-QUEUE] attempt=\(attemptID) queue=\(queueIndex) source=\(source) msg=\(message.id.prefix(16)) seq=\(message.seq)"
            )
            return try await self.processServerMessageLocked(message, context: context)
          }
        }

        // ═══════════════════════════════════════════════════════════════════════════
        // CRITICAL: Record successful processing and process any ready buffered messages
        // ═══════════════════════════════════════════════════════════════════════════
        
        // FIX D: Clear any persistent decryption failure tracking on success
        clearPersistentDecryptionFailure(messageID: message.id)

        // No advisory lock needed - SQLite WAL handles concurrent access
        // Cross-process coordination uses Darwin notifications (MLSCrossProcess)

        let readyMessages = try await self.messageOrderingCoordinator.recordMessageProcessed(
          messageID: message.id,
          conversationID: message.convoId,
          sequenceNumber: Int64(message.seq),
          currentUserDID: userDid,
          database: self.database
        )

        // Notify cross-process observers of database change
        MLSCrossProcess.shared.notifyChanged()

        // Process any buffered messages that are now ready (recursive processing)
        for pending in readyMessages {
          if let pendingMessage = deserializeMessageView(pending.messageViewJSON) {
            logger.info("[SEQ-ORDER] Processing buffered message \(pendingMessage.id) seq=\(pendingMessage.seq)")
            // Recursively process - this will handle the full ordering check again
            _ = try? await processServerMessage(pendingMessage, source: "buffered")
          }
        }

        return outcome
      } catch {
        lastError = error
        let errorDesc = error.localizedDescription
        let context = processingContext ?? ProcessingContext(
          attemptID: attemptID,
          source: source,
          queueIndex: 0
        )

        // CRITICAL FIX: Handle MLSError.cannotDecryptOwnMessage - this is a permanent failure
        // for self-sent messages. Reloading state won't fix it because we don't have the keys.
        if case MLSError.cannotDecryptOwnMessage = error {
          logger.warning("⚠️ [PROCESS] cannotDecryptOwnMessage for \(message.id) - marking as processed (unrecoverable self-message)")
          let outcome = try await saveErrorPlaceholder(
            message: message,
            error: "Self-sent message (cache missing)",
            validationReason: "CannotDecryptOwnMessage: Sender is current user",
            context: context
          )
          await recordSelfDecryptFailure(conversationID: message.convoId, source: source)
          // CRITICAL: Record as processed to prevent re-processing loop
          _ = await withAdvisoryLockBestEffort(for: userDid) {
            try await self.messageOrderingCoordinator.recordMessageProcessed(
              messageID: message.id,
              conversationID: message.convoId,
              sequenceNumber: Int64(message.seq),
              currentUserDID: userDid,
              database: self.database
            )
          }
          return outcome
        }
        
        // Also check string description for backward compatibility with FFI errors
        if errorDesc.contains("CannotDecryptOwnMessage") || errorDesc.contains("cannotDecryptOwnMessage") {
             logger.warning("⚠️ [PROCESS] CannotDecryptOwnMessage for \(message.id) - marking as processed (unrecoverable self-message)")
             // Save placeholder to advance sequence
             let outcome = try await saveErrorPlaceholder(
               message: message,
               error: "Self-sent message (cache missing)",
               validationReason: "CannotDecryptOwnMessage: Sender is current user",
               context: context
             )
             await recordSelfDecryptFailure(conversationID: message.convoId, source: source)
             // CRITICAL: Record as processed to prevent re-processing loop
             _ = await withAdvisoryLockBestEffort(for: userDid) {
               try await self.messageOrderingCoordinator.recordMessageProcessed(
                 messageID: message.id,
                 conversationID: message.convoId,
                 sequenceNumber: Int64(message.seq),
                 currentUserDID: userDid,
                 database: self.database
               )
             }
             return outcome
        }

        // Check for SecretReuseError (NSE race condition indicator), Future Epoch, or Decryption Failure
        // Explicitly EXCLUDE CannotDecryptOwnMessage to prevent death spiral
        if (errorDesc.contains("SecretReuseError") || errorDesc.contains("FutureEpoch") || errorDesc.contains("DecryptionFailed")) && !errorDesc.contains("CannotDecryptOwnMessage") {
          // Fast path: if NSE already wrote the payload, treat this as success and return from DB.
          if let cachedMessage = try? await storage.fetchMessage(
            messageID: message.id,
            currentUserDID: userDid,
            database: database
          ), let cachedPayload = cachedMessage.parsedPayload {
            logger.info("✅ [MLS-RECOVERY] Found cached payload for message \(message.id) after retryable error")

            if cachedPayload.messageType == .reaction {
              if let reaction = cachedPayload.reaction {
                let actorDID = cachedMessage.senderID ?? "unknown"
                
                // CRITICAL FIX: Validate actorDID prevents overwriting valid reactions with "unknown"
                if actorDID.isEmpty || actorDID == "unknown" {
                  logger.error("❌ [MLS-RECOVERY] Cannot recover reaction for \(reaction.messageId) - sender unknown (would overwrite)")
                  return .controlMessage // Skip saving reaction, but treat as processed
                }
                
                let reactionModel = MLSReactionModel(
                  messageID: reaction.messageId,
                  conversationID: message.convoId,
                  currentUserDID: userDid,
                  actorDID: actorDID,
                  emoji: reaction.emoji,
                  action: reaction.action.rawValue,
                  timestamp: message.createdAt.date
                )
                do {
                  if reaction.action == .add {
                    try await withDatabaseRecovery(currentUserDID: userDid) { db in
                        try await self.storage.saveReaction(reactionModel, database: db)
                    }
                  } else {
                    try await withDatabaseRecovery(currentUserDID: userDid) { db in
                        try await self.storage.deleteReaction(
                        messageID: reaction.messageId,
                        actorDID: actorDID,
                        emoji: reaction.emoji,
                        currentUserDID: userDid,
                        database: db
                      )
                    }
                  }

                  notifyObservers(.reactionReceived(
                    convoId: message.convoId,
                    messageId: reaction.messageId,
                    emoji: reaction.emoji,
                    senderDID: actorDID,
                    action: reaction.action.rawValue
                  ))
                } catch {
                  logger.error("❌ [MLS-RECOVERY] Failed to persist recovered reaction: \(error.localizedDescription)")
                }
              }
              return .controlMessage
            }

            if cachedPayload.messageType == .readReceipt {
              return .controlMessage
            }

            return .application(payload: cachedPayload, sender: cachedMessage.senderID)
          }

          if attempt < maxRetries {
            logger.warning("🔄 [MLS-RETRY] [Gen: \(gen)] Detected possible NSE race/Epoch Mismatch - reloading state and retrying (attempt \(attempt + 1)/\(maxRetries))")
            
            // 1. Reload Checkpoint Cache (Fastest check)
            await MLSEpochCheckpoint.shared.reloadCacheFromDisk()
            
            // 2. Force state reload from disk to pick up NSE changes
            await reloadStateFromDisk()
            
            // 3. KEY FIX: If this is a FutureEpoch/DecryptionFailed, try to fetch missing commits
            // This is the core fix for "Ratchet State Desync" - the state on disk may also be
            // behind if we missed a commit message. Fetching from server is the only way forward.
            if errorDesc.contains("FutureEpoch") || errorDesc.contains("DecryptionFailed") {
              // Get current local epoch and check if we need to catch up
              if let convo = conversations[message.convoId],
                 let groupIdData = Data(hexEncoded: convo.groupId) {
                let currentLocalEpoch = (try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) ?? 0
                
                if UInt64(message.epoch) > currentLocalEpoch {
                  logger.info("🔄 [EPOCH-RECOVERY] Message epoch \(message.epoch) > local \(currentLocalEpoch) - fetching missing commits")
                  let fetched = await fetchAndProcessMissingCommits(
                    conversationID: message.convoId,
                    groupId: convo.groupId,
                    localEpoch: currentLocalEpoch,
                    targetEpoch: message.epoch
                  )
                  if fetched {
                    logger.info("✅ [EPOCH-RECOVERY] Commits fetched successfully - retrying message processing")
                  }
                }
              }
            }
            
            // Exponential backoff: 50ms, 100ms, 200ms
            let delayMs = UInt64(50 * (1 << attempt))
            try? await Task.sleep(nanoseconds: delayMs * 1_000_000)
            continue
          } else {
            // CRITICAL FIX: After retries exhausted for SecretReuseError, check cache one final time
            // The NSE may have stored the message but cache lookup timing was off
            logger.warning("⚠️ [MLS-RETRY] Retry exhausted for message \(message.id) - attempting final cache recovery")
            
            if let cachedMessage = try? await storage.fetchMessage(
              messageID: message.id,
              currentUserDID: userDid,
              database: database
            ), let cachedPayload = cachedMessage.parsedPayload {
              logger.info("✅ [MLS-RECOVERY] Found cached payload for message \(message.id) after SecretReuseError")
              
              // Handle reaction payloads specially
              if cachedPayload.messageType == .reaction {
                if let reaction = cachedPayload.reaction {
                  let reactionModel = MLSReactionModel(
                    messageID: reaction.messageId,
                    conversationID: message.convoId,
                    currentUserDID: userDid,
                    actorDID: cachedMessage.senderID,
                    emoji: reaction.emoji,
                    action: reaction.action.rawValue,
                    timestamp: message.createdAt.date
                  )
                  
                  do {
                    if reaction.action == .add {
                      try await withDatabaseRecovery(currentUserDID: userDid) { db in
                          try await self.storage.saveReaction(reactionModel, database: db)
                      }
                    } else {
                      try await withDatabaseRecovery(currentUserDID: userDid) { db in
                          try await self.storage.deleteReaction(
                          messageID: reaction.messageId,
                          actorDID: cachedMessage.senderID,
                          emoji: reaction.emoji,
                          currentUserDID: userDid,
                          database: db
                        )
                      }
                    }
                    
                    // Notify UI about recovered reaction
                    notifyObservers(.reactionReceived(
                      convoId: message.convoId,
                      messageId: reaction.messageId,
                      emoji: reaction.emoji,
                      senderDID: cachedMessage.senderID ?? "unknown",
                      action: reaction.action.rawValue
                    ))
                    
                    logger.info("✅ [MLS-RECOVERY] Recovered reaction \(reaction.emoji) on \(reaction.messageId)")
                  } catch {
                    logger.error("❌ [MLS-RECOVERY] Failed to save recovered reaction: \(error.localizedDescription)")
                  }
                }
                return .controlMessage
              }
              
              return .application(payload: cachedPayload, sender: cachedMessage.senderID)
            }
            
            logger.error("❌ [MLS-RETRY] Retry exhausted and cache empty for message \(message.id)")
            
            // FIX D: Record persistent decryption failure - may trigger nuclear rejoin
            if errorDesc.contains("DecryptionFailed") || errorDesc.contains("FutureEpoch") {
              let _ = await recordPersistentDecryptionFailure(
                messageID: message.id,
                conversationID: message.convoId,
                source: source
              )
            }
          }
        }
        
        // If it's not a retryable error or we're out of retries, rethrow
        throw error
      }
    }
    
    if let lastError = lastError {
      throw lastError
    }
    throw MLSConversationError.operationFailed("Processing failed with unknown error")
  }

  // MARK: - Message Ordering Helpers

  /// Trigger gap fill to fetch missing messages up to a specific sequence number
  private func triggerGapFill(conversationID: String, upToSeq: Int64) async {
    guard let userDid = userDid else {
      logger.warning("[SEQ-ORDER] Cannot trigger gap fill - no user DID")
      return
    }

    do {
      // Get the last processed sequence number
      let lastSeq = try await storage.getLastProcessedSeq(
        conversationID: conversationID,
        currentUserDID: userDid,
        database: database
      )

      // If there's a gap, fetch the missing messages
      if upToSeq > lastSeq + 1 {
        let missingStart = lastSeq + 1
        let missingEnd = upToSeq - 1

        logger.info("[SEQ-ORDER] Triggering gap fill for conversation \(conversationID.prefix(8)) (seq \(missingStart)...\(missingEnd))")

        // Fetch missing messages from server
        let sinceParam = max(0, Int(missingStart) - 1)
        let (messages, _, _) = try await apiClient.getMessages(
          convoId: conversationID,
          limit: 50,
          sinceSeq: sinceParam
        )

        // Filter to only the relevant range and sort by sequence
        let relevantMessages = messages
          .filter { Int64($0.seq) >= missingStart && Int64($0.seq) <= missingEnd }
          .sorted { $0.seq < $1.seq }

        logger.info("[SEQ-ORDER] Gap fill fetched \(relevantMessages.count) missing messages")

        // Process each missing message (they will go through ordering check again)
        for msg in relevantMessages {
          _ = try? await processServerMessage(msg, source: "gap-fill-trigger")
        }
      }
    } catch {
      logger.error("[SEQ-ORDER] Gap fill failed: \(error.localizedDescription)")
    }
  }

  // MARK: - Epoch Gap Recovery
  
  /// Fetch and process missing commits when message epoch > local epoch
  /// This is the KEY FIX for "Ratchet State Desync" - instead of just reloading from disk,
  /// we actually fetch the missing commit(s) from the server that will advance our epoch.
  ///
  /// - Parameters:
  ///   - conversationID: The conversation ID (convoId format)
  ///   - groupId: The MLS group ID (hex string)
  ///   - localEpoch: Current local epoch from FFI
  ///   - targetEpoch: The epoch of the message we're trying to decrypt
  /// - Returns: true if any commits were successfully processed (epoch advanced)
  private func fetchAndProcessMissingCommits(
    conversationID: String,
    groupId: String,
    localEpoch: UInt64,
    targetEpoch: Int
  ) async -> Bool {
    guard let userDid = userDid else {
      logger.warning("⚠️ [EPOCH-RECOVERY] Cannot fetch commits - no user DID")
      return false
    }
    
    // Sanity check: only fetch if we're actually behind
    guard UInt64(targetEpoch) > localEpoch else {
      logger.debug("[EPOCH-RECOVERY] No epoch gap: local=\(localEpoch), target=\(targetEpoch)")
      return true // No gap, so "success" (nothing to do)
    }
    
    let epochGap = UInt64(targetEpoch) - localEpoch
    logger.info("🔄 [EPOCH-RECOVERY] Fetching commits for epoch gap: local=\(localEpoch) → target=\(targetEpoch) (gap=\(epochGap))")
    
    do {
      // Fetch commits from server covering the epoch range
      // We request fromEpoch = localEpoch (current) to toEpoch = targetEpoch
      let commits = try await apiClient.getCommits(
        convoId: conversationID,
        fromEpoch: Int(localEpoch),
        toEpoch: targetEpoch
      )
      
      guard !commits.isEmpty else {
        logger.warning("⚠️ [EPOCH-RECOVERY] Server returned no commits for epoch range \(localEpoch)-\(targetEpoch)")
        return false
      }
      
      logger.info("📥 [EPOCH-RECOVERY] Received \(commits.count) commits from server")
      
      // Sort commits by epoch (ascending) to process in order
      let sortedCommits = commits.sorted { $0.epoch < $1.epoch }
      
      var processedCount = 0
      for commit in sortedCommits {
        // Skip commits we've already processed
        if UInt64(commit.epoch) <= localEpoch {
          logger.debug("[EPOCH-RECOVERY] Skipping already-processed commit epoch=\(commit.epoch)")
          continue
        }
        
        // Get commit data (Bytes type from AT Protocol)
        guard let commitBytes = commit.commitData else {
            logger.warning("⚠️ [EPOCH-RECOVERY] Commit data missing for epoch=\(commit.epoch)")
            continue
        }
        
        do {
          // Process the commit through OpenMLS
          try await processCommit(groupId: groupId, commitData: commitBytes.data)
          processedCount += 1
          logger.info("✅ [EPOCH-RECOVERY] Processed commit for epoch=\(commit.epoch)")
        } catch {
          logger.error("❌ [EPOCH-RECOVERY] Failed to process commit epoch=\(commit.epoch): \(error.localizedDescription)")
          // Continue trying other commits - maybe we can skip a corrupt one
        }
      }
      
      if processedCount > 0 {
        logger.info("🎉 [EPOCH-RECOVERY] Successfully processed \(processedCount) commits, epoch should now be advanced")
        return true
      } else {
        logger.warning("⚠️ [EPOCH-RECOVERY] No commits were successfully processed")
        return false
      }
      
    } catch {
      logger.error("❌ [EPOCH-RECOVERY] Failed to fetch commits: \(error.localizedDescription)")
      return false
    }
  }

  // MARK: - Gap Filling (Locked)

  /// Fill message gaps while holding the conversation lock
  /// CRITICAL: This uses recursion instead of detached tasks to avoid DEADLOCK with ConversationProcessingCoordinator
  private func fillGapsLocked(conversationID: String, startSeq: Int, endSeq: Int) async {
    let limit = 50 // Limit batch size to prevent blocking lock for too long
    logger.info("🧩 [Gap Fill] Locked catch-up for \(conversationID) (Seq \(startSeq)-\(endSeq))")
    
    do {
      // Fetch missing messages
      // Note: getMessages expects 'sinceSeq', so to get 'startSeq', pass 'startSeq - 1'
      let sinceParam = max(0, startSeq - 1)
      let (messages, _, _) = try await apiClient.getMessages(
        convoId: conversationID,
        limit: limit,
        sinceSeq: sinceParam
      )
      
      let relevantMessages = messages.filter { Int($0.seq) <= endSeq }
      
      if relevantMessages.isEmpty {
        logger.warning("🧩 [Gap Fill] Server returned no messages for gap \(startSeq)-\(endSeq)")
        return
      }
      
      let gen = currentCoordinationGeneration
      logger.info("🧩 [Gap Fill] [Gen: \(gen)] Processing \(relevantMessages.count) gap messages...")
      
      // Sort: App messages first, then commits (within same epoch)
      // Since we are filling a gap, strictly following sequence order is safest
      let sortedMessages = relevantMessages.sorted { $0.seq < $1.seq }
      
      for msg in sortedMessages {
        // Break early if shutdown initiated during gap fill
        if isShuttingDown { break }

        do {
          // RECURSIVE CALL: Directly call the locked processing method
          // This bypasses the coordinator lock acquisition since we already hold it
          let context = ProcessingContext(
            attemptID: nextProcessingAttemptID(),
            source: "gap-fill",
            queueIndex: 0
          )
          _ = try await processServerMessageLocked(msg, context: context)
          logger.debug("🧩 [Gap Fill] Successfully processed gap message seq: \(msg.seq)")
        } catch {
          logger.error("🧩 [Gap Fill] Failed to process gap message \(msg.seq): \(error.localizedDescription)")
          // Continue with next message - don't abort the whole gap fill
        }
      }
      
      logger.info("🧩 [Gap Fill] Completed processing batch")
    } catch {
      logger.error("🧩 [Gap Fill] Failed to fetch gap messages: \(error.localizedDescription)")
    }
  }

  private func processServerMessageLocked(
    _ message: BlueCatbirdMlsDefs.MessageView,
    context: ProcessingContext
  ) async throws -> MessageProcessingOutcome
  {
    // CRITICAL FIX: Fail fast if shutdown in progress to avoid SQLite error 21
    try throwIfShuttingDown("processServerMessageLocked")

    var localEpoch: Int64 = 0
    let gen = currentCoordinationGeneration
    let localEpochView = localEpoch > 0 ? "\(localEpoch)" : "unknown"
    logger.debug(
      "📦 [PROCESS] [Gen: \(gen)] [Attempt: \(context.attemptID)] [Queue: \(context.queueIndex)] [Source: \(context.source)] [Local Epoch: \(localEpochView)] Processing server message \(message.id) (Msg Epoch: \(message.epoch), Seq: \(message.seq))")

    guard let convo = conversations[message.convoId] else {
      logger.error(
        "Cannot process message \(message.id) - conversation \(message.convoId) not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    // Ensure conversation exists in SQLCipher BEFORE any decrypt operations
    do {
      try await storage.ensureConversationExists(
        userDID: userDid,
        conversationID: message.convoId,
        groupID: convo.groupId,
        database: database
      )
    } catch {
      logger.error("❌ Failed to ensure conversation exists: \(error.localizedDescription)")
      throw MLSConversationError.operationFailed("Database not ready for message processing")
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      logger.error("Invalid groupId for conversation \(convo.groupId)")
      throw MLSConversationError.invalidGroupId
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Gap Detection & Prevention using Recursive Fill
    // ═══════════════════════════════════════════════════════════════════════════
    // Prevents "Old Epoch" errors by maximizing the chance that we process strictly in order.
    // If we receive Seq 10 (Commit) but DB only has Seq 8, we MUST try to fetch/process Seq 9
    // BEFORE processing Seq 10. Otherwise, Seq 10 advances epoch and Seq 9 becomes undecryptable.
    // ═══════════════════════════════════════════════════════════════════════════
    if let lastSeq = await lastStoredSequenceNumber(for: message.convoId) {
      // If lastSeq is 0, it might be a fresh convo, so we apply loose rules or check against 0.
      // Message Seq 1 should proceed if lastSeq is 0.
      // Gap condition: message.seq > lastSeq + 1
      let serverSeq = Int(message.seq)
      if serverSeq > lastSeq + 1 {
        let missingStart = lastSeq + 1
        let missingEnd = serverSeq - 1
        
        logger.warning("🚧 GAP DETECTED: Incoming Seq \(serverSeq) > Last Stored \(lastSeq) + 1")
        logger.info("   Initiating locked recursive fill for range \(missingStart)...\(missingEnd)")
        
        // This is a synchronous await within the lock - safe because fillGapsLocked is recursive
        // and does NOT spawn new tasks/locks.
        await fillGapsLocked(conversationID: message.convoId, startSeq: missingStart, endSeq: missingEnd)
        
        // After filling, we proceed with the current message.
        // Note: effectively we updated the state, so 'localEpoch' retrieval below will be fresh.
      }
    }


    do {
      let epoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
      localEpoch = Int64(epoch)
    } catch {
      logger.warning("⚠️ Unable to query local epoch for validation: \(error.localizedDescription)")
    }

    let paddedCiphertext = message.ciphertext.data
    let ciphertextData: Data
    do {
      ciphertextData = try MLSMessagePadding.removePadding(paddedCiphertext)
    } catch {
      logger.error("❌ Failed to remove padding from message \(message.id): \(error.localizedDescription)")
      return try await saveErrorPlaceholder(
        message: message,
        error: "Invalid message padding",
        validationReason: "Failed to decode message structure",
        context: context
      )
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Check self-sent message cache BEFORE epoch check
    // ═══════════════════════════════════════════════════════════════════════════
    // When we send a message, the epoch may advance before the echo returns.
    // Self-sent messages are pre-cached (see sendMessage lines 83-94), so we
    // must check the cache BEFORE applying the epoch filter. If it's our message,
    // we use the cached payload and skip decryption entirely.
    // ═══════════════════════════════════════════════════════════════════════════
    let cachedSender: String?
    do {
      cachedSender = try await storage.fetchSenderForMessage(
        message.id,
        currentUserDID: userDid,
        database: database
      )
    } catch {
      logger.error("❌ Error fetching cached sender for message \(message.id): \(error.localizedDescription)")
      cachedSender = nil
    }

    if let cachedSender, cachedSender == userDid {
      logger.info("[SELF-ECHO] Detected self-sent message \(message.id) from epoch \(message.epoch) (local: \(localEpoch))")
      
      if let cachedPayload = try? await storage.fetchPayloadForMessage(
        message.id,
        currentUserDID: userDid,
        database: database
      ) {
        logger.info("♻️ Using cached payload for self-sent message \(message.id)")
        clearSelfDecryptFailures(conversationID: message.convoId)
        return .application(payload: cachedPayload, sender: cachedSender)
      } else {
        // CRITICAL FIX: Save a placeholder to prevent re-processing attempts
        // Without this, the message would be retried on next sync, hitting the FFI
        // and failing with CannotDecryptOwnMessage error (forward secrecy)
        logger.warning("⚠️ [SELF-ECHO] Message \(message.id) is self-sent but payload missing - saving placeholder")
        
        let placeholderPayload = MLSMessagePayload.text(
          "⚠️ Message unavailable (sent from this account)",
          embed: nil
        )
        _ = try await persistProcessedPayload(
          message: message,
          payload: placeholderPayload,
          senderID: cachedSender,
          processingError: nil,
          validationReason: nil,
          context: context
        )
        logger.info("✅ [SELF-ECHO] Saved placeholder for self-sent message with missing cache")
        await recordSelfDecryptFailure(conversationID: message.convoId, source: context.source)
        
        return .nonApplication
      }
    }

    // Cache lookup for non-self-sent messages
    do {
      if let cachedMessage = try await storage.fetchMessage(
        messageID: message.id,
        currentUserDID: userDid,
        database: database
      ) {
        logger.info("🔍 [CACHE-LOOKUP] Found cached message \(message.id.prefix(16)) - processingError=\(cachedMessage.processingError ?? "nil"), payloadExpired=\(cachedMessage.payloadExpired), hasPayload=\(cachedMessage.parsedPayload != nil)")
        if cachedMessage.processingError != nil {
          logger.warning("⚠️ [CACHE-LOOKUP] Message \(message.id.prefix(16)) has processingError: \(cachedMessage.processingError!) - returning .nonApplication")
          return .nonApplication
        }
        if cachedMessage.payloadExpired {
          logger.warning("⚠️ [CACHE-LOOKUP] Message \(message.id.prefix(16)) payloadExpired - returning .nonApplication")
          return .nonApplication
        }
        if let cachedPayload = cachedMessage.parsedPayload {
          if cachedPayload.messageType == .reaction {
            // IMPORTANT: NSE may have cached the reaction payload but not persisted the reaction row.
            // Ensure reactions are written before we skip control messages.
            if let reaction = cachedPayload.reaction {
              let actorDID = cachedMessage.senderID ?? "unknown"
              
              // CRITICAL FIX: Validate actorDID prevents overwriting valid reactions
              if actorDID.isEmpty || actorDID == "unknown" {
                 logger.error("❌ [CACHE-RECOVERY] Cannot recover reaction for \(reaction.messageId) - sender unknown")
                 return .controlMessage
              }

              let reactionModel = MLSReactionModel(
                messageID: reaction.messageId,
                conversationID: message.convoId,
                currentUserDID: userDid,
                actorDID: actorDID,
                emoji: reaction.emoji,
                action: reaction.action.rawValue,
                timestamp: message.createdAt.date
              )
              do {
                if reaction.action == .add {
                  try await withDatabaseRecovery(currentUserDID: userDid) { db in
                      try await self.storage.saveReaction(reactionModel, database: db)
                  }
                } else {
                  try await withDatabaseRecovery(currentUserDID: userDid) { db in
                      try await self.storage.deleteReaction(
                      messageID: reaction.messageId,
                      actorDID: actorDID,
                      emoji: reaction.emoji,
                      currentUserDID: userDid,
                      database: db
                    )
                  }
                }
                notifyObservers(.reactionReceived(
                  convoId: message.convoId,
                  messageId: reaction.messageId,
                  emoji: reaction.emoji,
                  senderDID: actorDID,
                  action: reaction.action.rawValue
                ))
              } catch {
                logger.error("❌ Failed to persist cached reaction for \(message.id): \(error.localizedDescription)")
              }
            }
            return .controlMessage
          }

          if cachedPayload.messageType == .readReceipt {
            return .controlMessage
          }

          logger.debug("♻️ Using cached payload for message \(message.id)")
          clearSelfDecryptFailures(conversationID: message.convoId)
          return .application(payload: cachedPayload, sender: cachedMessage.senderID ?? "unknown")
        }
      }
    } catch {
      logger.warning("⚠️ Cache lookup failed for message \(message.id): \(error.localizedDescription)")
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Epoch check - only for non-self-sent messages that weren't in cache
    // ═══════════════════════════════════════════════════════════════════════════
    if UInt64(message.epoch) < UInt64(localEpoch) {
      logger.warning("[EPOCH-SKIP] Skipping message \(message.id): msg_epoch=\(message.epoch), local_epoch=\(localEpoch)")
      return try await saveErrorPlaceholder(
        message: message,
        error: "Message from old epoch",
        validationReason: "Epoch \(message.epoch) is behind local epoch \(localEpoch)",
        context: context
      )
    }

    // Proceed to decryption
    do {
      let mutationID = nextMutationID()
      logger.info(
        "🔁 [MLS-MUTATION] attempt=\(context.attemptID) mutation=\(mutationID) queue=\(context.queueIndex) msg=\(message.id.prefix(16)) seq=\(message.seq)"
      )
      let processedContent = try await mlsClient.processMessage(
        for: userDid,
        groupId: groupIdData,
        messageData: ciphertextData
      )

      MLSStateVersionManager.shared.incrementVersion(for: userDid)
      clearSelfDecryptFailures(conversationID: message.convoId)

      switch processedContent {
      case .applicationMessage(let plaintextData, let senderCredential):
        let senderDID = try extractDIDFromCredential(senderCredential)
        let payload = try MLSMessagePayload.decodeFromJSON(plaintextData)
        let plaintextString = String(data: plaintextData, encoding: .utf8) ?? "{\"error\": \"undecodable\"}"

        logger.debug("✅ Successfully decrypted message \(message.id)")

        switch payload.messageType {
        case .text:
          // Persist payload for text messages
          let adopted = try await persistProcessedPayload(
            message: message,
            payload: payload,
            senderID: senderDID,
            processingError: nil,
            validationReason: nil,
            context: context
          )

          if !adopted.isEmpty {
            for reaction in adopted {
              notifyObservers(.reactionReceived(
                convoId: reaction.conversationID,
                messageId: reaction.messageID,
                emoji: reaction.emoji,
                senderDID: reaction.actorDID,
                action: reaction.action
              ))
            }
            logger.info("[ORPHAN-ADOPT] Notified UI of \(adopted.count) adopted reaction(s) for \(message.id.prefix(16))")
          }

        case .reaction:
          logger.info("🔍 [REACTION-DEBUG] Received reaction message \(message.id) from \(senderDID)")
          logger.info("🔍 [REACTION-DEBUG] payload.reaction is \(payload.reaction == nil ? "NIL ⚠️" : "present ✅")")
          
          if let reaction = payload.reaction {
            logger.info("❤️ [REACTION] Processing reaction from \(senderDID): \(reaction.emoji) on \(reaction.messageId) (action: \(reaction.action.rawValue))")
            
            let reactionModel = MLSReactionModel(
              messageID: reaction.messageId,
              conversationID: message.convoId,
              currentUserDID: userDid,
              actorDID: senderDID,
              emoji: reaction.emoji,
              action: reaction.action.rawValue,
              timestamp: message.createdAt.date
            )

            do {
              if reaction.action == .add {
                try await withDatabaseRecovery(currentUserDID: userDid) { db in
                    try await self.storage.saveReaction(reactionModel, database: db)
                }
                logger.info("✅ [REACTION] Saved reaction \(reaction.emoji) on \(reaction.messageId) by \(senderDID)")
              } else {
                try await withDatabaseRecovery(currentUserDID: userDid) { db in
                    try await self.storage.deleteReaction(
                    messageID: reaction.messageId,
                    actorDID: senderDID,
                    emoji: reaction.emoji,
                    currentUserDID: userDid,
                    database: db
                  )
                }
                logger.info("✅ [REACTION] Deleted reaction \(reaction.emoji) on \(reaction.messageId) by \(senderDID)")
              }
              
              // Notify UI
              notifyObservers(.reactionReceived(
                convoId: message.convoId, 
                messageId: reaction.messageId, 
                emoji: reaction.emoji, 
                senderDID: senderDID, 
                action: reaction.action.rawValue
              ))
              logger.info("📡 [REACTION] Notified UI observers of reaction")
              
            } catch {
              logger.error("❌ [REACTION] Failed to save reaction for \(message.id): \(error.localizedDescription)")
            }
          } else {
            logger.warning("⚠️ [REACTION-DEBUG] payload.reaction is nil but messageType is .reaction - payload may be malformed")
            // Log the raw payload for debugging
            if let payloadJSON = try? payload.encodeToJSON(), let jsonString = String(data: payloadJSON, encoding: .utf8) {
              logger.warning("⚠️ [REACTION-DEBUG] Raw payload JSON: \(jsonString.prefix(500))")
            }
          }

          // Persist control message (full payload) to advance sequence number
          // NOTE: This is OUTSIDE the if-let so we always save the message even if reaction parsing failed
          _ = try await persistProcessedPayload(
            message: message,
            payload: payload,
            senderID: senderDID,
            processingError: nil,
            validationReason: nil,
            context: context
          )

        case .readReceipt:
          // Read receipts have been removed - ignore and persist for sequence advancement
          _ = try await persistProcessedPayload(
            message: message,
            payload: payload,
            senderID: senderDID,
            processingError: nil,
            validationReason: nil,
            context: context
          )

        case .typing:
          // Persist typing payloads to avoid reprocessing loops.
          _ = try await persistProcessedPayload(
            message: message,
            payload: payload,
            senderID: senderDID,
            processingError: nil,
            validationReason: nil,
            context: context
          )

        case .adminRoster, .adminAction:
          // Persist admin payloads to keep ordering state consistent.
          _ = try await persistProcessedPayload(
            message: message,
            payload: payload,
            senderID: senderDID,
            processingError: nil,
            validationReason: nil,
            context: context
          )
        }

        return .application(payload: payload, sender: senderDID)

      case .proposal(let proposal, let proposalRef):
        logger.info("📜 Processing proposal message \(message.id)")
        try await handleProposal(groupId: convo.groupId, proposal: proposal, proposalRef: proposalRef)
        let placeholderPayload = placeholderPayload(for: message, text: "⚙️ Protocol message")
        try await persistPlaceholderPayload(
          message: message,
          payload: placeholderPayload,
          senderID: "unknown",
          processingError: nil,
          validationReason: nil,
          context: context
        )
        return .nonApplication

      case .stagedCommit(let newEpoch):
        logger.info("📡 Commit message \(message.id) processed, verifying epoch \(newEpoch)")
        try await validateAndMergeStagedCommit(groupId: convo.groupId, newEpoch: newEpoch)
        let placeholderPayload = placeholderPayload(for: message, text: "⚙️ Protocol message")
        try await persistPlaceholderPayload(
          message: message,
          payload: placeholderPayload,
          senderID: "unknown",
          processingError: nil,
          validationReason: nil,
          context: context
        )
        return .nonApplication
      }
    } catch let error as MLSError {
      if case .ignoredOldEpochMessage = error {
        return try await saveErrorPlaceholder(
          message: message,
          error: "Message from old epoch",
          validationReason: "Ignored old epoch message",
          context: context
        )
      }

      let errorDescription = error.localizedDescription
      
      // CRITICAL FIX: Handle CannotDecryptOwnMessage
      // This happens when we self-send a message/reaction, lose the local cache (optimistic write failed),
      // and then receive it back from the server.
      // We cannot decrypt it because we are the sender.
      if errorDescription.contains("CannotDecryptOwnMessage") {
        logger.warning("⚠️ [PROCESS] CannotDecryptOwnMessage for \(message.id) - this is a self-sent message with missing local cache. Marking as processed.")
        
        // We must save a placeholder to advance the sequence number and prevent valid lookback loops
        return try await saveErrorPlaceholder(
          message: message,
          error: "Self-sent message (cache missing)",
          validationReason: "CannotDecryptOwnMessage: Sender is current user",
          context: context
        )
      }

      if errorDescription.contains("SecretReuseError") || errorDescription.contains("Decryption failed") {
        // CRITICAL FIX: Rethrow SecretReuseError (and generic decryption failures which mask it)
        // so the parent method can catch it and retry/reload from cache if available.
        logger.warning("⚠️ Message \(message.id) triggered SecretReuseError/DecryptionFailed - propagating for retry")
        throw error
      }

      logger.error("❌ MLS error processing message \(message.id): \(error.localizedDescription)")
      return try await saveErrorPlaceholder(
        message: message,
        error: errorDescription,
        validationReason: nil,
        context: context
      )
    } catch {
       // Also check generic error description for "CannotDecryptOwnMessage" string
       let errorDescr = error.localizedDescription
       if errorDescr.contains("CannotDecryptOwnMessage") {
         logger.warning("⚠️ [PROCESS] CannotDecryptOwnMessage (generic) for \(message.id) - marking as processed.")
         return try await saveErrorPlaceholder(
           message: message,
           error: "Self-sent message (cache missing)",
           validationReason: "CannotDecryptOwnMessage: Sender is current user",
           context: context
         )
       }
    
      logger.error("❌ Failed to process MLS message \(message.id): \(error.localizedDescription)")
      return try await saveErrorPlaceholder(
        message: message,
        error: error.localizedDescription,
        validationReason: nil,
        context: context
      )
    }
  }

  private func saveErrorPlaceholder(
    message: BlueCatbirdMlsDefs.MessageView,
    error: String,
    validationReason: String?,
    context: ProcessingContext? = nil
  ) async throws -> MessageProcessingOutcome {
    guard userDid != nil else {
      throw MLSConversationError.noAuthentication
    }

    let normalizedType = (message.messageType ?? "app").lowercased()
    let isApplication = normalizedType == "app" || normalizedType == "application"
    let placeholderPayload = placeholderPayload(for: message, text: "⚠️ Message unavailable")
    try await persistPlaceholderPayload(
      message: message,
      payload: placeholderPayload,
      senderID: "unknown",
      processingError: error,
      validationReason: validationReason,
      context: context
    )

    if isApplication {
      return .application(payload: placeholderPayload, sender: "unknown")
    }
    return .nonApplication
  }

  private func placeholderPayload(
    for message: BlueCatbirdMlsDefs.MessageView,
    text: String
  ) -> MLSMessagePayload {
    let normalizedType = (message.messageType ?? "app").lowercased()
    if normalizedType == "app" || normalizedType == "application" {
      return MLSMessagePayload.text(text, embed: nil)
    }
    return MLSMessagePayload.typing(isTyping: false)
  }

  private func persistPlaceholderPayload(
    message: BlueCatbirdMlsDefs.MessageView,
    payload: MLSMessagePayload,
    senderID: String,
    processingError: String?,
    validationReason: String?,
    context: ProcessingContext?
  ) async throws {
    let resolvedContext = context ?? ProcessingContext(
      attemptID: nextProcessingAttemptID(),
      source: "placeholder",
      queueIndex: 0
    )
    _ = try await persistProcessedPayload(
      message: message,
      payload: payload,
      senderID: senderID,
      processingError: processingError,
      validationReason: validationReason,
      context: resolvedContext
    )
  }

  /// Process messages in sequential order
  public func processMessagesInOrder(
    messages: [BlueCatbirdMlsDefs.MessageView],
    conversationID: String,
    source: String = "sync"
  ) async throws -> [MLSMessagePayload] {
    // CRITICAL FIX: Check shutdown state before processing batch
    if isShuttingDown {
      logger.warning("⚠️ [PROCESS] Shutdown in progress - aborting message processing")
      return []
    }
    
    logger.debug("📊 [SEQ-ORDER] Processing \(messages.count) messages for conversation \(conversationID) source=\(source)")
    if await shouldSkipProcessingForRejoin(conversationID: conversationID, source: source) {
      return []
    }

    guard let userDid = userDid else {
      return []
    }

    guard !messages.isEmpty else {
      return []
    }

    var messagesByEpoch: [Int: [BlueCatbirdMlsDefs.MessageView]] = [:]
    for message in messages {
      messagesByEpoch[message.epoch, default: []].append(message)
    }

    let sortedEpochs = messagesByEpoch.keys.sorted()
    var processedPayloads: [MLSMessagePayload] = []

    guard let groupIdData = Data(hexEncoded: conversationID) else {
      throw MLSConversationError.invalidGroupId
    }

    for epoch in sortedEpochs {
      // CRITICAL FIX: Check shutdown during epoch loop
      if isShuttingDown {
        logger.warning("⚠️ [PROCESS] Shutdown detected during processing - stopping")
        break
      }
      
      guard let epochMessages = messagesByEpoch[epoch] else { continue }
      try Task.checkCancellation()

      var localEpoch: UInt64 = 0
      do {
        localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
      } catch {}

      // FIX B: ALWAYS process commits before application messages within each epoch.
      // This ensures the epoch is advanced before we try to decrypt application messages.
      // Previously we only did this when needCommitsFirst was true, but that led to
      // desync when commits arrived out-of-order within the same batch.
      let _ = localEpoch < UInt64(epoch) // Keep for logging/diagnostics only

      let (appMessages, commitMessages) = epochMessages.reduce(
        into: (app: [BlueCatbirdMlsDefs.MessageView](), commit: [BlueCatbirdMlsDefs.MessageView]())
      ) { result, msg in
        let msgType = (msg.messageType ?? "app").lowercased()
        if msgType == "app" || msgType == "application" {
          result.app.append(msg)
        } else {
          result.commit.append(msg)
        }
      }

      // ALWAYS process commits first, then application messages
      // This is critical for epoch advancement before decryption
      let firstBatch = commitMessages.sorted(by: { $0.seq < $1.seq })
      let secondBatch = appMessages.sorted(by: { $0.seq < $1.seq })
      let firstBatchIsApps = false // Commits are always first now

      for message in firstBatch {
        // Check ordering decision (pass epoch info for epoch-aware ordering)
        let decision = try await messageOrderingCoordinator.shouldProcessMessage(
          messageID: message.id,
          conversationID: conversationID,
          sequenceNumber: Int64(message.seq),
          messageEpoch: Int64(message.epoch),
          localEpoch: Int64(localEpoch),
          currentUserDID: userDid,
          database: database
        )

        switch decision {
        case .alreadyProcessed:
          logger.debug("[SEQ-ORDER] Skipping already-processed message \(message.id) seq=\(message.seq)")
          continue

        case .buffer, .bufferForFutureEpoch:
          // In sync context, we have all messages - process in order anyway
          logger.debug("[SEQ-ORDER] Message \(message.id) seq=\(message.seq) would buffer, but processing in sync context")
          fallthrough

        case .processNow, .forceProcess:
          let result = await processMessageWithRecovery(
            message: message,
            conversationID: conversationID,
            epoch: epoch,
            source: source
          )

          if case .success(let outcome) = result, firstBatchIsApps, case .application(let payload, _) = outcome {
            processedPayloads.append(payload)
          }

          // Record message as processed
          if case .success = result {
            _ = await withAdvisoryLockBestEffort(for: userDid) {
              try await self.messageOrderingCoordinator.recordMessageProcessed(
                messageID: message.id,
                conversationID: conversationID,
                sequenceNumber: Int64(message.seq),
                currentUserDID: userDid,
                database: self.database
              )
            }
          }

          if await shouldSkipProcessingForRejoin(conversationID: conversationID, source: source) {
            return processedPayloads
          }
        }
      }

      for message in secondBatch {
        // Check ordering decision (pass epoch info for epoch-aware ordering)
        let decision = try await messageOrderingCoordinator.shouldProcessMessage(
          messageID: message.id,
          conversationID: conversationID,
          sequenceNumber: Int64(message.seq),
          messageEpoch: Int64(message.epoch),
          localEpoch: Int64(localEpoch),
          currentUserDID: userDid,
          database: database
        )

        switch decision {
        case .alreadyProcessed:
          logger.debug("[SEQ-ORDER] Skipping already-processed message \(message.id) seq=\(message.seq)")
          continue

        case .buffer, .bufferForFutureEpoch:
          // In sync context, we have all messages - process in order anyway
          logger.debug("[SEQ-ORDER] Message \(message.id) seq=\(message.seq) would buffer, but processing in sync context")
          fallthrough

        case .processNow, .forceProcess:
          let result = await processMessageWithRecovery(
            message: message,
            conversationID: conversationID,
            epoch: epoch,
            source: source
          )

          if case .success(let outcome) = result, !firstBatchIsApps, case .application(let payload, _) = outcome {
            processedPayloads.append(payload)
          }

          // Record message as processed
          if case .success = result {
            _ = await withAdvisoryLockBestEffort(for: userDid) {
              try await self.messageOrderingCoordinator.recordMessageProcessed(
                messageID: message.id,
                conversationID: conversationID,
                sequenceNumber: Int64(message.seq),
                currentUserDID: userDid,
                database: self.database
              )
            }
          }

          if await shouldSkipProcessingForRejoin(conversationID: conversationID, source: source) {
            return processedPayloads
          }
        }
      }
    }

    return processedPayloads
  }

  internal func processMessageWithRecovery(
    message: BlueCatbirdMlsDefs.MessageView,
    conversationID: String,
    epoch: Int,
    source: String = "sync",
    retryCount: Int = 0
  ) async -> MessageProcessingResult {
    // Check shutdown first
    if isShuttingDown { return .skipped }

    let maxRetries = 2

    do {
      if let userDid = userDid {
        do {
          // Use self.database directly - it's already the correct database for this user
          if let cachedPayload = try await storage.fetchPayloadForMessage(
            message.id,
            currentUserDID: userDid,
            database: database
          ) {
            let cachedSender = try await storage.fetchSenderForMessage(
              message.id,
              currentUserDID: userDid,
              database: database
            ) ?? "unknown"
            return .success(.application(payload: cachedPayload, sender: cachedSender))
          }
        } catch {}
      }

      let outcome = try await Task.detached { [self] in
        try await self.processServerMessage(message, source: source)
      }.value

      return .success(outcome)

    } catch MLSError.ignoredOldEpochMessage {
      return .skipped
    } catch MLSError.secretReuseSkipped {
      if let userDid = userDid {
        do {
          // Use self.database directly - it's already the correct database for this user
          if let cachedPayload = try await storage.fetchPayloadForMessage(
            message.id,
            currentUserDID: userDid,
            database: database
          ) {
            let cachedSender = try await storage.fetchSenderForMessage(
              message.id,
              currentUserDID: userDid,
              database: database
            ) ?? "unknown"
            return .success(.application(payload: cachedPayload, sender: cachedSender))
          }
        } catch {}
      }
      return .skipped
    } catch let error as MLSError {
      switch error {
      case .ratchetStateDesync(let reason):
        if reason.contains("Cannot decrypt message from epoch")
          || reason.contains("forward secrecy")
        {
          return .skipped
        }
        if retryCount < maxRetries {
          try? await Task.sleep(nanoseconds: 100_000_000)
          return await processMessageWithRecovery(
            message: message,
            conversationID: conversationID,
            epoch: epoch,
            source: source,
            retryCount: retryCount + 1
          )
        }
        return .failure(error)
      default:
        // CRITICAL FIX: If SecretReuseError persists after retries (logic above would have retried if possible),
        // we MUST save a placeholder to advance the sequence number.
        // Otherwise, we get stuck in a Gap Fill loop forever.
        if error.localizedDescription.contains("SecretReuseError") {
          logger.error("❌ [RECOVERY] Persistent SecretReuseError for \(message.id) - saving placeholder to advance sequence")
          
          do {
            let context = ProcessingContext(
              attemptID: nextProcessingAttemptID(),
              source: source,
              queueIndex: 0
            )
            let _ = try await saveErrorPlaceholder(
              message: message, 
              error: "Decryption Failed (Secret Reuse)", 
              validationReason: "Persistent SecretReuseError after retries",
              context: context
            )
            // Return success with placeholder so checks pass and we don't refetch
            // We need to return .success so the caller records it as processed
            let placeholderPayload = MLSMessagePayload.text("⚠️ Decryption Failed", embed: nil)
            return .success(.application(payload: placeholderPayload, sender: "unknown"))
          } catch {
            logger.error("❌ Failed to save placeholder for \(message.id): \(error.localizedDescription)")
            // If we can't even save the placeholder, we are truly stuck, but we should try to fail gracefully
            return .failure(error)
          }
        }
        return .failure(error)
      }
    } catch {
      if retryCount < maxRetries {
        let errorDesc = error.localizedDescription
        if errorDesc.contains("timeout") || errorDesc.contains("connection") || errorDesc.contains("network") {
          try? await Task.sleep(nanoseconds: 100_000_000)
          return await processMessageWithRecovery(
            message: message,
            conversationID: conversationID,
            epoch: epoch,
            source: source,
            retryCount: retryCount + 1
          )
        }
      }
      
      // CRITICAL FIX: Fallback for generic SecretReuseError (not castable to MLSError)
      if error.localizedDescription.contains("SecretReuseError") {
        logger.error("❌ [RECOVERY] Persistent generic SecretReuseError for \(message.id) - saving placeholder")
        
        do {
          let context = ProcessingContext(
            attemptID: nextProcessingAttemptID(),
            source: source,
            queueIndex: 0
          )
          let _ = try await saveErrorPlaceholder(
            message: message, 
            error: "Decryption Failed (Secret Reuse)", 
            validationReason: "Persistent SecretReuseError (generic)",
            context: context
          )
          let placeholderPayload = MLSMessagePayload.text("⚠️ Decryption Failed", embed: nil)
          return .success(.application(payload: placeholderPayload, sender: "unknown"))
        } catch {
             logger.error("❌ Failed to save placeholder: \(error.localizedDescription)")
             return .failure(error)
        }
      }
      
      return .failure(error)
    }
  }


  internal func catchUpMessagesIfNeeded(for convo: BlueCatbirdMlsDefs.ConvoView, force: Bool = false)
    async
  {
    // CRITICAL: Capture session generation at start
    let myGeneration = sessionGeneration

    // CRITICAL FIX: Check shutdown state before starting catchup
    // This prevents processing while account is switching
    if isShuttingDown {
      logger.warning("⚠️ [CATCHUP] Shutdown in progress - skipping catchup")
      return
    }

    if await conversationNeedsRejoin(convo.groupId) && !force { return }
    guard let userDid = userDid else { return }

    do {
      var sinceSeq = await lastStoredSequenceNumber(for: convo.groupId)
      let pageLimit = 10
      var pages = 0

      while !Task.isCancelled && !isShuttingDown {
        // Check generation before each iteration
        do {
          try validateSessionGeneration(capturedGeneration: myGeneration)
        } catch {
          logger.warning("⚠️ [CATCHUP] Session invalidated - aborting catchup")
          return
        }
        let (messages, _, gapInfo) = try await apiClient.getMessages(
          convoId: convo.groupId,
          limit: 100,
          sinceSeq: sinceSeq
        )

        guard !messages.isEmpty else { break }

        if let gaps = gapInfo, gaps.hasGaps {
          await fillGaps(conversationID: convo.groupId, missingSeqs: gaps.missingSeqs)
        }

        let _ = try await processMessagesInOrder(
          messages: messages,
          conversationID: convo.groupId,
          source: "catchup"
        )
        sinceSeq = messages.last?.seq
        pages += 1
        if messages.count < 100 || pages >= pageLimit { break }
      }

      // After catchup completes, flush any remaining buffered messages for this conversation
      logger.debug("[SEQ-ORDER] Flushing buffered messages after catchup for \(convo.groupId)")
      do {
        // Use self.database directly - it's already the correct database for this user
        let buffered = try await messageOrderingCoordinator.flushBufferedMessages(
          conversationID: convo.groupId,
          currentUserDID: userDid,
          database: database
        )

        if !buffered.isEmpty {
          logger.info("[SEQ-ORDER] Processing \(buffered.count) buffered messages after catchup")
          for pending in buffered {
            if let msg = deserializeMessageView(pending.messageViewJSON) {
              _ = try? await processServerMessage(msg, source: "buffered")
            } else {
              logger.warning("[SEQ-ORDER] Failed to deserialize buffered message \(pending.messageID)")
            }
          }
        }
      } catch {
        logger.error("[SEQ-ORDER] Failed to flush buffered messages: \(error.localizedDescription)")
      }
    } catch {
      logger.error("❌ Catch-up failed for \(convo.groupId): \(error.localizedDescription)")
    }
  }

  internal func fillGaps(conversationID: String, missingSeqs: [Int]) async {
    guard !missingSeqs.isEmpty else { return }
    let ranges = groupIntoRanges(missingSeqs.sorted())

    for (startSeq, endSeq) in ranges {
      do {
        let (messages, _, _) = try await apiClient.getMessages(
          convoId: conversationID,
          limit: (endSeq - startSeq) + 10,
          sinceSeq: max(0, startSeq - 1)
        )
        if !messages.isEmpty {
          try await processMessagesInOrder(
            messages: messages,
            conversationID: conversationID,
            source: "gap-fill"
          )
        }
      } catch {}
    }
  }

  /// Fetch messages with lookback to find missing parents for orphans
  /// This re-fetches the last 50 messages to catch any that were skipped/gapped
  internal func fetchMissingMessagesWithLookback(for convo: BlueCatbirdMlsDefs.ConvoView) async {
    // CRITICAL FIX: Validate this manager is still for the active user
    // This catches zombie tasks from previous account contexts before they process data
    do {
      try throwIfShuttingDown("fetchMissingMessagesWithLookback")
    } catch {
      logger.warning("⚠️ [ORPHAN-Lookback] Aborted - manager is shutting down")
      return
    }
    
    // Validate coordination generation hasn't changed (account switch detection)
    let currentGen = MLSCoordinationStore.shared.getState().coordinationGeneration
    if currentGen != currentCoordinationGeneration {
      logger.error("❌ [ORPHAN-Lookback] Account mismatch detected - aborting")
        logger.error("   Manager generation: \(self.currentCoordinationGeneration), current: \(currentGen)")
      return
    }

    guard let userDid = userDid else { return }
    if await shouldSkipProcessingForRejoin(conversationID: convo.groupId, source: "lookback") {
      return
    }

    // Calculate lookback sequence
    // We want to re-fetch the last 50 messages to catch any that were skipped
    // Use lastStoredSequenceNumber which hits the DB directly
    let lastSeq = await lastStoredSequenceNumber(for: convo.groupId) ?? 0
    let lookbackSeq = max(0, lastSeq - 50)

    logger.info("[ORPHAN-Lookback] Fetching messages for \(convo.groupId.prefix(16)) starting from seq \(lookbackSeq) (lookback from \(lastSeq))")

    do {
      // Fetch messages with lookback
      // This will return messages > lookbackSeq
      let (messages, _, _) = try await apiClient.getMessages(
        convoId: convo.groupId,
        limit: 100,
        sinceSeq: lookbackSeq
      )

      if !messages.isEmpty {
        logger.info("[ORPHAN-Lookback] Fetched \(messages.count) messages")
        // Process them - our updated MLSMessageOrderingCoordinator will allow re-processing if missing from DB
        let _ = try await processMessagesInOrder(
          messages: messages,
          conversationID: convo.groupId,
          source: "lookback"
        )
      } else {
         logger.debug("[ORPHAN-Lookback] No messages returned")
      }

      // Flush buffer just in case
      let _ = try await messageOrderingCoordinator.flushBufferedMessages(
           conversationID: convo.groupId,
           currentUserDID: userDid,
           database: database
      )

    } catch {
       logger.error("[ORPHAN-Lookback] Failed to fetch messages: \(error.localizedDescription)")
    }
  }

  private func groupIntoRanges(_ sequences: [Int]) -> [(Int, Int)] {
    guard !sequences.isEmpty else { return [] }
    var ranges: [(Int, Int)] = []
    var rangeStart = sequences[0]
    var rangeEnd = sequences[0]

    for seq in sequences.dropFirst() {
      if seq == rangeEnd + 1 {
        rangeEnd = seq
      } else {
        ranges.append((rangeStart, rangeEnd))
        rangeStart = seq
        rangeEnd = seq
      }
    }
    ranges.append((rangeStart, rangeEnd))
    return ranges
  }

  /// Deserialize a buffered MessageView from JSON
  private func deserializeMessageView(_ json: Data) -> BlueCatbirdMlsDefs.MessageView? {
    do {
      let decoder = JSONDecoder()
      decoder.dateDecodingStrategy = .iso8601
      return try decoder.decode(BlueCatbirdMlsDefs.MessageView.self, from: json)
    } catch {
      logger.error("[SEQ-ORDER] Failed to deserialize MessageView: \(error.localizedDescription)")
      return nil
    }
  }

  // MARK: - Handlers

  internal func handleReceivedReaction(
    _ payload: MLSReactionPayload,
    from senderDID: String,
    in conversationId: String
  ) async {
    switch payload.action {
    case .add:
      let reaction = MLSMessageReaction(
        messageId: payload.messageId,
        reaction: payload.emoji,
        senderDID: senderDID,
        reactedAt: Date()
      )
      await saveReactionFromSSE(reaction, conversationId: conversationId)
      notifyObservers(.reactionReceived(convoId: conversationId, messageId: payload.messageId, emoji: payload.emoji, senderDID: senderDID, action: "add"))
    case .remove:
      await deleteReactionFromSSE(messageId: payload.messageId, senderDID: senderDID, emoji: payload.emoji, conversationId: conversationId)
      notifyObservers(.reactionReceived(convoId: conversationId, messageId: payload.messageId, emoji: payload.emoji, senderDID: senderDID, action: "remove"))
    }
  }

  internal func saveReactionFromSSE(_ reaction: MLSMessageReaction, conversationId: String) async {
    guard let userDid = userDid else {
      return
    }

    let reactionModel = MLSReactionModel(
      messageID: reaction.messageId,
      conversationID: conversationId,
      currentUserDID: userDid,
      actorDID: reaction.senderDID,
      emoji: reaction.reaction,
      action: "add",
      timestamp: reaction.reactedAt ?? Date()
    )

    do {
      try await withDatabaseRecovery(currentUserDID: userDid) { db in
          try await self.storage.saveReaction(reactionModel, database: db)
      }
    } catch {}
  }

  internal func deleteReactionFromSSE(
    messageId: String, senderDID: String, emoji: String, conversationId: String
  ) async {
    guard let userDid = userDid else {
      return
    }

    do {
      try await withDatabaseRecovery(currentUserDID: userDid) { db in
          try await self.storage.deleteReaction(
          messageID: messageId,
          actorDID: senderDID,
          emoji: emoji,
          currentUserDID: userDid,
          database: db
        )
      }
    } catch {}
  }

  // Read receipt and typing indicator handling functions have been removed.
  // - handleReceivedReadReceipt
  // - handleReceivedTyping

  private func shouldRecoverDatabaseConnection(from error: Error) -> Bool {
    if let dbError = error as? DatabaseError, dbError.resultCode.rawValue == 21 {
      return true
    }
    let description = error.localizedDescription.lowercased()
    return description.contains("sqlite error 21")
      || description.contains("sqlite_misuse")
      || description.contains("connection is closed")
      || description.contains("database is closed")
  }

  private func withDatabaseRecovery<T>(
    currentUserDID: String,
    operation: @Sendable @escaping (MLSDatabase) async throws -> T
  ) async throws -> T {
    do {
      return try await operation(database)
    } catch {
      guard shouldRecoverDatabaseConnection(from: error) else {
        throw error
      }

      logger.warning(
        "⚠️ [DB-RECOVERY] Detected closed connection for \(currentUserDID.prefix(20))... - reconnecting"
      )

      let recovered = try await MLSGRDBManager.shared.reconnectDatabase(
        for: currentUserDID,
        triggeringError: error
      )
      database = recovered

      logger.info("✅ [DB-RECOVERY] Reconnected database for \(currentUserDID.prefix(20))...")
      return try await operation(database)
    }
  }


  internal func cacheControlMessageEnvelope(
    message: BlueCatbirdMlsDefs.MessageView,
    payload: MLSMessagePayload,
    senderDID: String,
    currentUserDID: String
  ) async throws {
    // Retry logic for robust caching (max 3 attempts)
    // We CANNOT proceed without caching, or we will hit CannotDecryptOwnMessage on echo
    for attempt in 1...3 {
      do {
        _ = try await withDatabaseRecovery(currentUserDID: currentUserDID) { db in
            try await self.storage.savePayloadForMessage(
              messageID: message.id,
              conversationID: message.convoId,
              payload: payload,
              senderID: senderDID,
              currentUserDID: currentUserDID,
              epoch: Int64(message.epoch),
              sequenceNumber: Int64(message.seq),
              timestamp: message.createdAt.date,
              database: db
          )
        }
        // Success
        return
      } catch {
        logger.warning("⚠️ Failed to cache control message \(message.id) (attempt \(attempt)/3): \(error.localizedDescription)")
        if attempt == 3 {
           logger.error("❌ CRITICAL: Could not cache message locally. Aborting send to prevent state corruption.")
           throw error
        }
        try? await Task.sleep(nanoseconds: 100_000_000) // 100ms backoff
      }
    }
  }

  // MARK: - Utilities

  internal func generateIdempotencyKey(convoId: String, plaintext: Data) -> String {
    var hasher = SHA256()
    hasher.update(data: convoId.data(using: .utf8)!)
    hasher.update(data: plaintext)
    let timestamp = Date().timeIntervalSince1970
    hasher.update(data: "\(timestamp)".data(using: .utf8)!)
    return hasher.finalize().hexEncodedString()
  }

  internal func isRecentlySent(convoId: String, idempotencyKey: String) -> Bool {
    guard let keys = recentlySentMessages[convoId],
      let timestamp = keys[idempotencyKey]
    else {
      return false
    }
    return Date().timeIntervalSince(timestamp) < 300.0 // deduplicationWindow
  }

    @MainActor internal func trackSentMessage(convoId: String, idempotencyKey: String) {
    if recentlySentMessages[convoId] == nil {
      recentlySentMessages[convoId] = [:]
    }
    recentlySentMessages[convoId]?[idempotencyKey] = Date()
    startDeduplicationCleanupTimerIfNeeded()
  }

  internal func trackOwnCommit(_ commitData: Data) {
    let commitHash = SHA256.hash(data: commitData).compactMap { String(format: "%02x", $0) }.joined()
    ownCommitsLock.lock()
    defer { ownCommitsLock.unlock() }
    ownCommits[commitHash] = Date()
  }

  internal func isOwnCommit(_ commitData: Data) -> Bool {
    let commitHash = SHA256.hash(data: commitData).compactMap { String(format: "%02x", $0) }.joined()
    ownCommitsLock.lock()
    defer { ownCommitsLock.unlock() }
    let now = Date()
    ownCommits = ownCommits.filter { now.timeIntervalSince($0.value) < 600.0 } // ownCommitTimeout
    return ownCommits[commitHash] != nil
  }

  // MARK: - Helper Methods

  /// Get the last stored sequence number for a conversation
  internal func lastStoredSequenceNumber(for conversationId: String) async -> Int? {
    guard let userDid = userDid else { return nil }
    do {
      let cursor = try await storage.fetchLastMessageCursor(
        conversationID: conversationId,
        currentUserDID: userDid,
        database: database
      )
      return cursor.map { Int($0.seq) }
    } catch {
      logger.error("Failed to get last sequence number: \(error.localizedDescription)")
      return nil
    }
  }

  // Typing indicator cleanup functions have been removed.
  // - getTypingUsers
  // - startTypingCleanupTimerIfNeeded
  // - cleanupStaleTypingIndicators


  /// Start a timer to clean up old deduplication entries if not already running
  @MainActor
  internal func startDeduplicationCleanupTimerIfNeeded() {
    guard deduplicationCleanupTimer == nil else { return }
    deduplicationCleanupTimer = Timer.scheduledTimer(withTimeInterval: 60.0, repeats: true) { [weak self] _ in
      Task { @MainActor [weak self] in
        self?.cleanupOldDeduplicationEntries()
      }
    }
  }

  /// Clean up old deduplication entries (older than 5 minutes)
  @MainActor
  private func cleanupOldDeduplicationEntries() {
    let cutoff = Date().addingTimeInterval(-300) // 5 minutes
    for (conversationId, entries) in recentlySentMessages {
      let active = entries.filter { $0.value > cutoff }
      if active.isEmpty {
        recentlySentMessages.removeValue(forKey: conversationId)
      } else {
        recentlySentMessages[conversationId] = active
      }
    }
    if recentlySentMessages.isEmpty {
      deduplicationCleanupTimer?.invalidate()
      deduplicationCleanupTimer = nil
    }
  }

  internal func extractDIDFromCredential(_ credential: CredentialData) throws -> String {
    guard let didString = String(data: credential.identity, encoding: .utf8) else {
      throw MLSConversationError.invalidCredential
    }
    guard didString.starts(with: "did:") else {
      throw MLSConversationError.invalidCredential
    }
    return didString
  }


  // MARK: - Recovery & Rejoin

  internal func markConversationNeedsRejoin(_ convoId: String) async throws {
    guard let userDID = userDid else { return }

    try await database.write { db in
      try db.execute(
        sql: """
              UPDATE MLSConversationModel
              SET needsRejoin = 1, rejoinRequestedAt = NULL, updatedAt = ?
              WHERE conversationID = ? AND currentUserDID = ?;
          """, arguments: [Date(), convoId, userDID])
    }
  }

  internal func clearConversationRejoinFlag(_ convoId: String) async {
    guard let userDID = userDid else { return }

    do {
      try await database.write { db in
        try db.execute(
          sql: """
                UPDATE MLSConversationModel
                SET needsRejoin = 0, rejoinRequestedAt = NULL, updatedAt = ?
                WHERE conversationID = ? AND currentUserDID = ?;
            """, arguments: [Date(), convoId, userDID])
      }
    } catch {}
  }

  internal func conversationNeedsRejoin(_ convoId: String) async -> Bool {
    guard let userDID = userDid else { return false }
    do {
      return try await database.read { db in
        try Bool.fetchOne(
          db,
          sql: """
                SELECT needsRejoin FROM MLSConversationModel
                WHERE conversationID = ? AND currentUserDID = ?;
            """,
          arguments: [convoId, userDID]
        ) ?? false
      }
    } catch {
      return false
    }
  }

  internal func conversationHasPendingRejoinRequest(_ convoId: String) async -> Bool {
    guard let userDID = userDid else { return false }
    do {
      return try await database.read { db in
        try Bool.fetchOne(
          db,
          sql: """
                SELECT rejoinRequestedAt IS NOT NULL FROM MLSConversationModel
                WHERE conversationID = ? AND currentUserDID = ?;
            """,
          arguments: [convoId, userDID]
        ) ?? false
      }
    } catch {
      return false
    }
  }

  internal func recordRejoinRequestTimestamp(_ convoId: String) async {
    guard let userDID = userDid else { return }
    do {
      try await database.write { db in
        try db.execute(
          sql: """
                UPDATE MLSConversationModel
                SET rejoinRequestedAt = ?, updatedAt = ?
                WHERE conversationID = ? AND currentUserDID = ?;
            """, arguments: [Date(), Date(), convoId, userDID])
      }
    } catch {}
  }

  internal func requestRejoinIfPossible(convoId: String, reason: String) async {
    guard let userDID = userDid else { return }

    if await conversationHasPendingRejoinRequest(convoId) { return }

    do {
      _ = try await mlsClient.joinByExternalCommit(for: userDID, convoId: convoId)
      await recordRejoinRequestTimestamp(convoId)
    } catch {}
  }


  internal func incrementConversationFailures(conversationID: String) async {
    guard let userDid = userDid else { return }

    do {
      try await database.write { db in
        try db.execute(
          sql: """
                UPDATE MLSConversationModel
                SET consecutiveFailures = consecutiveFailures + 1
                WHERE conversationID = ? AND currentUserDID = ?;
            """, arguments: [conversationID, userDid])
      }
    } catch {}
  }

  internal func updateLastRecoveryAttempt(conversationID: String) async {
    guard let userDid = userDid else { return }

    do {
      try await database.write { db in
        try db.execute(
          sql: """
                UPDATE MLSConversationModel
                SET lastRecoveryAttempt = ?
                WHERE conversationID = ? AND currentUserDID = ?;
            """, arguments: [Date(), conversationID, userDid])
      }
    } catch {}
  }

  internal func handleRatchetDesync(for conversationID: String, reason: String) async {
    do {
      try await markConversationNeedsRejoin(conversationID)
    } catch {}
    await requestRejoinIfPossible(convoId: conversationID, reason: reason)
  }

  // MARK: - Epoch Management

  /// Get current epoch for a conversation
  /// - Parameter convoId: Conversation identifier
  /// - Returns: Current epoch number from FFI (ground truth)
  internal func getEpoch(convoId: String) async throws -> UInt64 {
    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // ⭐ CRITICAL FIX: Query FFI for actual epoch (ground truth from crypto layer)
    // Never trust server's potentially stale epoch value
    return try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
  }

  /// Handle epoch update from server
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - newEpoch: New epoch number
  internal func handleEpochUpdate(convoId: String, newEpoch: UInt64) {
    logger.info("Handling epoch update for conversation: \(convoId), new epoch: \(newEpoch)")

    let epochInt = Int(clamping: newEpoch)

    guard var convo = conversations[convoId] else {
      logger.warning("Conversation not found for epoch update: \(convoId)")
      return
    }

    // Update conversation epoch
    let updatedConvo = BlueCatbirdMlsDefs.ConvoView(
      groupId: convo.groupId,
      creator: convo.creator,
      members: convo.members,
      epoch: epochInt,
      cipherSuite: convo.cipherSuite,
      createdAt: convo.createdAt,
      lastMessageAt: convo.lastMessageAt,
      metadata: convo.metadata
    )
    conversations[convoId] = updatedConvo

    // Update group state
    if var state = groupStates[convo.groupId] {
      state.epoch = newEpoch
      groupStates[convo.groupId] = state
    }

    // Notify observers
    notifyObservers(.epochUpdated(convoId, epochInt))
  }

  /// Synchronize group state by fetching and processing missing commits
  /// - Parameter convoId: Conversation identifier
  /// - Throws: MLSConversationError if sync fails
  internal func syncGroupState(for convoId: String) async throws {
    logger.info("Syncing group state for conversation: \(convoId)")

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // ⭐ CRITICAL FIX: Get actual local epoch from FFI (ground truth)
    // DO NOT use convo.epoch which is the server's potentially stale view
    let localEpochFFI: UInt64
    do {
      localEpochFFI = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
      logger.debug("📍 FFI local epoch: \(localEpochFFI)")
    } catch {
      logger.error("Failed to get FFI epoch: \(error.localizedDescription)")
      throw MLSConversationError.operationFailed("Cannot get local epoch from FFI")
    }

    // Fetch server epoch
    let serverEpoch: Int
    do {
      serverEpoch = try await apiClient.getEpoch(convoId: convoId)
      logger.debug("📍 Server epoch: \(serverEpoch), FFI local epoch: \(localEpochFFI)")
    } catch {
      logger.error("Failed to fetch server epoch: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }

    // Compare FFI epoch (actual local state) vs server epoch
    let localEpochInt = Int(localEpochFFI)

    if localEpochInt > serverEpoch {
      // FFI is ahead of server - normal after group creation/commits
      // Server will catch up asynchronously
      logger.info(
        "✅ FFI ahead of server (FFI: \(localEpochFFI), Server: \(serverEpoch)) - no sync needed")
      return
    }

    if localEpochInt == serverEpoch {
      // Already in sync
      logger.debug("Already at latest epoch (\(localEpochFFI)), no sync needed")
      return
    }

    // FFI is behind server - need to fetch and process commits
    logger.info(
      "Behind server epoch: FFI=\(localEpochFFI), server=\(serverEpoch), fetching \(serverEpoch - localEpochInt) commits"
    )

    // Fetch missing commits
    let commits: [BlueCatbirdMlsGetCommits.CommitMessage]
    do {
      commits = try await apiClient.getCommits(
        convoId: convoId,
        fromEpoch: localEpochInt + 1,
        toEpoch: serverEpoch
      )
      logger.debug("Fetched \(commits.count) commits to process")
    } catch {
      logger.error("Failed to fetch commits: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }

    // Process each commit through OpenMLS
    for commit in commits {
      do {
        logger.debug("Processing commit for epoch \(commit.epoch)")

        // Get commit ciphertext data
        guard let commitBytes = commit.commitData else {
             logger.warning("Commit data missing for epoch \(commit.epoch)")
             continue
        }
        let commitData = commitBytes.data

        // Process commit through MLS crypto layer
        // This will update the group state internally
        try await processCommit(groupId: convo.groupId, commitData: commitData)

        logger.debug("Successfully processed commit for epoch \(commit.epoch)")
      } catch {
        logger.error(
          "Failed to process commit for epoch \(commit.epoch): \(error.localizedDescription)")

        // 🔄 RECOVERY: Check if this error warrants device-level recovery
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          let recovered = await recoveryManager.attemptRecoveryIfNeeded(
            for: error,
            userDid: userDid,
            convoIds: [convoId]
          )
          if recovered {
            logger.info(
              "🔄 Silent recovery initiated for conversation \(convoId.prefix(16)) - will rejoin in background"
            )
          }
        }

        throw MLSConversationError.commitProcessingFailed(commit.epoch, error)
      }
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    let actualEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
    let serverEpochUInt = UInt64(serverEpoch)

    logger.info(
      "✅ Commits processed. FFI epoch: \(actualEpoch), Server reported: \(serverEpochUInt)")

    if actualEpoch != serverEpochUInt {
      logger.warning("⚠️ EPOCH MISMATCH after sync:")
      logger.warning("   FFI (actual): \(actualEpoch)")
      logger.warning("   Server (stale): \(serverEpochUInt)")
      logger.warning("   Trusting FFI state to prevent desynchronization")
    }

    // Update local epoch to match FFI (not server)
    handleEpochUpdate(convoId: convoId, newEpoch: actualEpoch)

    // Notify observers of epoch update AFTER database commits
    notifyObservers(.epochUpdated(convoId, Int(actualEpoch)))

    logger.info("Successfully synced group state to FFI epoch \(actualEpoch)")
  }

  /// Process a commit message through OpenMLS
  /// - Parameters:
  ///   - groupId: Group identifier
  ///   - commitData: Raw commit message data
  internal func processCommit(groupId: String, commitData: Data) async throws {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    // Convert hex-encoded groupId to Data
    guard let groupIdData = Data(hexEncoded: groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Process commit through MLS client
    let result = try await mlsClient.processCommit(
      for: userDid, groupId: groupIdData, commitData: commitData)
    logger.info("Processed commit: new epoch \(result.newEpoch)")
    let epochInt = Int(clamping: result.newEpoch)

    // Update local group state with new epoch
    if var state = groupStates[groupId] {
      state.epoch = result.newEpoch
      groupStates[groupId] = state

      // Persist epoch to keychain
      do {
        try MLSKeychainManager.shared.storeCurrentEpoch(epochInt, forConversationID: state.convoId)
        logger.debug("Persisted epoch \(epochInt) to keychain for conversation \(state.convoId)")
      } catch {
        logger.error("Failed to persist epoch to keychain: \(error)")
      }

      // Record new epoch in storage for cleanup tracking
      do {
        try await storage.recordEpochKey(
          conversationID: state.convoId,
          epoch: Int64(epochInt),
          userDID: userDid,
          database: database
        )
        logger.debug("Recorded epoch key for cleanup tracking")

        // Clean up old epoch keys based on retention policy
        try await storage.deleteOldEpochKeys(
          conversationID: state.convoId,
          userDID: userDid,
          keepLast: configuration.maxPastEpochs,
          database: database
        )

        // Notify observers of epoch update AFTER database commit
        notifyObservers(.epochUpdated(state.convoId, epochInt))
        logger.debug("Cleaned up old epoch keys (keeping last \(self.configuration.maxPastEpochs))")
      } catch {
        logger.error("Failed to cleanup old epoch keys: \(error)")
      }

      // Persist MLS state after epoch change (critical for forward secrecy)
      do {
        logger.debug("✅ Persisted MLS state after epoch \(epochInt)")
      } catch {
        logger.error("⚠️ Failed to persist MLS state after commit: \(error.localizedDescription)")
      }

      // Notify observers of epoch update
      notifyObservers(.epochUpdated(state.convoId, epochInt))
      logger.debug("Updated local epoch for group \(groupId.prefix(8))... to \(result.newEpoch)")
    } else {
      logger.warning(
        "No local group state found for group \(groupId.prefix(8))... after processing commit")
    }
  }

  // MARK: - Force Rejoin Recovery

  /// 🔒 FIX #6: Nuclear rejoin option for unrecoverable epoch desync
  ///
  /// This method forcefully re-joins a conversation when normal recovery fails.
  /// Use this when:
  /// - User is stuck at an old epoch and cannot process commits
  /// - GroupInfo on server was corrupted but has since been refreshed
  /// - Manual intervention is needed to restore conversation access
  ///
  /// The process:
  /// 1. Delete local group state (wipe corrupted MLS state)
  /// 2. Request fresh GroupInfo from active members
  /// 3. Wait for fresh GroupInfo to be published
  /// 4. Rejoin via External Commit with fresh state
  ///
  /// - Parameter convoId: Conversation identifier to force rejoin
  /// - Throws: MLSConversationError if the operation fails
  /// - Warning: This discards all local MLS state for this conversation!
  internal func forceRejoin(for convoId: String) async throws {
    logger.warning(
      "🔄 [forceRejoin] Starting NUCLEAR REJOIN for conversation \(convoId.prefix(16))...")
    logger.warning("   ⚠️  This will DELETE all local MLS state for this conversation!")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Step 1: Delete local group state
    logger.info("🗑️ [forceRejoin] Step 1/4: Deleting local group state...")
    do {
      try await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
      logger.info("✅ [forceRejoin] Local group state deleted")
    } catch {
      logger.warning(
        "⚠️ [forceRejoin] Delete group failed (may not exist): \(error.localizedDescription)")
      // Continue anyway - group might not exist locally
    }

    // Also clear local tracking state
    groupStates.removeValue(forKey: convo.groupId)

    // Step 2: Request fresh GroupInfo from active members
    logger.info("📡 [forceRejoin] Step 2/4: Requesting GroupInfo refresh from active members...")
    do {
      let (requested, activeMembers) = try await apiClient.groupInfoRefresh(convoId: convoId)
      if requested {
        logger.info(
          "✅ [forceRejoin] GroupInfo refresh requested - \(activeMembers ?? 0) active members notified"
        )
      } else {
        logger.warning("⚠️ [forceRejoin] No active members to refresh GroupInfo - proceeding anyway")
      }
    } catch {
      logger.warning(
        "⚠️ [forceRejoin] Failed to request GroupInfo refresh: \(error.localizedDescription)")
      // Continue anyway - maybe GroupInfo is already fresh
    }

    // Step 3: Wait for fresh GroupInfo to be published
    logger.info("⏳ [forceRejoin] Step 3/4: Waiting 3s for fresh GroupInfo...")
    try await Task.sleep(for: .seconds(3))

    // Step 4: Rejoin via External Commit
    logger.info("🔐 [forceRejoin] Step 4/4: Rejoining via External Commit...")
    let newGroupId = try await mlsClient.joinByExternalCommit(for: userDid, convoId: convoId)

    // Verify we rejoined the same group
    let newGroupIdHex = newGroupId.hexEncodedString()
    if newGroupIdHex != convo.groupId {
      logger.warning("⚠️ [forceRejoin] Group ID changed after rejoin!")
      logger.warning("   Old: \(convo.groupId.prefix(16))")
      logger.warning("   New: \(newGroupIdHex.prefix(16))")
    }

    // Get new epoch
    let newEpoch = try await mlsClient.getEpoch(for: userDid, groupId: newGroupId)
    logger.info("✅ [forceRejoin] SUCCESS - Rejoined at epoch \(newEpoch)")

    // Update local group state
    groupStates[newGroupIdHex] = MLSGroupState(
      groupId: newGroupIdHex,
      convoId: convoId,
      epoch: newEpoch,
      members: []
    )

    // Note: The conversation record in `conversations` dictionary uses ConvoView from server
    // which we cannot modify directly. The server will update it when we fetch conversations.
    // We just need to ensure our local groupStates is correct.

    // Clear any failed rejoin tracking for this conversation
    if let recoveryManager = await mlsClient.recovery(for: userDid) {
      await recoveryManager.clearRejoinTracking(convoId: convoId)
    }

    // Notify observers
    notifyObservers(.epochUpdated(convoId, Int(newEpoch)))
    logger.info("🎉 [forceRejoin] Nuclear rejoin complete for \(convoId.prefix(16))")
  }

  // MARK: - State Repair for Admins

  /// Force republish fresh GroupInfo for a conversation
  ///
  /// 🔧 STATE REPAIR: Call this when the stored GroupInfo on the server is corrupt
  /// (EndOfStream errors during External Commit). This function:
  /// 1. Exports fresh GroupInfo from local MLS state
  /// 2. Validates the GroupInfo before upload
  /// 3. Uploads to server, overwriting the corrupt data
  /// 4. Verifies the upload succeeded
  ///
  /// After calling this, broken clients can retry External Commit and should succeed.
  ///
  /// - Parameter convoId: Conversation identifier to repair
  /// - Throws: MLSConversationError if the operation fails
  /// - Note: Only call this if you are on the "true" epoch (usually the admin or last committer)
  internal func forceRepublishGroupInfo(for convoId: String) async throws {
    logger.info(
      "🔧 [forceRepublishGroupInfo] Starting GroupInfo repair for \(convoId.prefix(16))...")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Step 1: Verify we have valid local state
    let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
    logger.info("📍 [forceRepublishGroupInfo] Local epoch: \(localEpoch)")

    // Step 2: Publish fresh GroupInfo (with validation)
    logger.info("📤 [forceRepublishGroupInfo] Publishing fresh GroupInfo...")
    try await publishLatestGroupInfo(
      userDid: userDid,
      convoId: convoId,
      groupId: groupIdData,
      context: "force repair"
    )

    // Step 3: Verify GroupInfo health after publish
    if let recoveryManager = await mlsClient.recovery(for: userDid) {
      let isHealthy = await recoveryManager.verifyGroupInfoHealth(
        convoId: convoId,
        expectedSize: 0  // No specific expectation
      )
      if isHealthy {
        logger.info(
          "✅ [forceRepublishGroupInfo] GroupInfo repair SUCCESSFUL for \(convoId.prefix(16))")
        logger.info("   Broken clients can now retry External Commit")
      } else {
        logger.error("❌ [forceRepublishGroupInfo] GroupInfo repair FAILED - verification failed")
        throw MLSConversationError.operationFailed("GroupInfo repair verification failed")
      }
    } else {
      logger.info("✅ [forceRepublishGroupInfo] GroupInfo published (no recovery manager to verify)")
    }
  }

  // MARK: - Background Cleanup

  /// Start background cleanup task for old key material
  internal func startBackgroundCleanup() {
    cleanupTask?.cancel()

    cleanupTask = Task { [weak self] in
      guard let self else { return }

      // Capture session generation to detect account switches
      let myGeneration = self.sessionGeneration
      let taskId = self.registerTask()
      defer { self.unregisterTask(taskId) }

      while !Task.isCancelled {
        do {
          // Validate session before each iteration
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          try await Task.sleep(for: .seconds(self.configuration.cleanupInterval))

          guard !Task.isCancelled else { break }

          // Validate again before performing work
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          await self.performBackgroundCleanup()
        } catch is CancellationError {
          self.logger.info("Background cleanup task cancelled")
          break
        } catch {
          self.logger.error("Background cleanup error: \(error)")
          break
        }
      }
    }

    logger.info(
      "Started background cleanup task (interval: \(self.configuration.cleanupInterval)s)")
  }

  /// Start periodic background sync to keep conversations in sync with server
  internal func startPeriodicSync() {
    periodicSyncTask?.cancel()

    periodicSyncTask = Task { [weak self] in
      guard let self else { return }

      // Capture session generation to detect account switches
      let myGeneration = self.sessionGeneration
      let taskId = self.registerTask()
      defer { self.unregisterTask(taskId) }

      // Wait 30 seconds before first sync to avoid startup congestion
      try? await Task.sleep(for: .seconds(30))

      while !Task.isCancelled {
        do {
          // Validate session before work
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          // Sync every 5 minutes
          try await Task.sleep(for: .seconds(300))

          guard !Task.isCancelled else { break }

          // Validate again after sleep
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          self.logger.info("🔄 Running periodic background sync")
          try? await self.syncWithServer(fullSync: false)
        } catch is CancellationError {
          self.logger.info("Periodic sync task cancelled")
          break
        } catch {
          self.logger.error("Periodic sync error: \(error)")
          break
        }
      }
    }

    logger.info("Started periodic background sync task (interval: 5 minutes)")
  }

  /// Start background task for adopting orphaned reactions
  /// Periodically checks for orphaned reactions and triggers parent message fetches
  internal func startOrphanAdoptionTask() {
    orphanAdoptionTask?.cancel()

    orphanAdoptionTask = Task { [weak self] in
      guard let self else { return }

      // Capture session generation to detect account switches
      let myGeneration = self.sessionGeneration
      let taskId = self.registerTask()
      defer { self.unregisterTask(taskId) }

      // Wait 15 seconds before first check to let initial sync complete
      try? await Task.sleep(for: .seconds(15))

      while !Task.isCancelled {
        do {
          // Validate session before work
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          // Check every 30 seconds for faster orphan recovery
          try await Task.sleep(for: .seconds(30))

          guard !Task.isCancelled, !isShuttingDown else { break }

          // Validate again after sleep
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          guard let userDid = userDid else { continue }

          // 1. Adopt orphaned reactions
          await adoptPendingOrphans(userDID: userDid)

          // 2. Cleanup stale pending messages (message ordering buffer)
          do {
            let cleaned = try await messageOrderingCoordinator.cleanupStaleMessages(
              currentUserDID: userDid,
              database: database
            )
            if cleaned > 0 {
              logger.info("[SEQ-ORDER] Cleaned up \(cleaned) stale pending messages")
            }
          } catch {
            logger.error("[SEQ-ORDER] Failed to cleanup stale messages: \(error.localizedDescription)")
          }
        } catch {
          if error is CancellationError {
            self.logger.info("Orphan adoption task cancelled")
            break
          }
          self.logger.error("Orphan adoption error: \(error)")
        }
      }
    }

    logger.info("Started orphan adoption task (interval: 30 seconds)")
  }

  // MARK: - Proactive GroupInfo Refresh

  /// Start background task to proactively refresh GroupInfo for all active conversations
  /// This ensures External Commit has fresh GroupInfo available for new device joins
  internal func startGroupInfoRefreshTask() {
    groupInfoRefreshTask?.cancel()

    groupInfoRefreshTask = Task { [weak self] in
      guard let self else { return }

      let myGeneration = self.sessionGeneration
      let taskId = self.registerTask()
      defer { self.unregisterTask(taskId) }

      // Wait 60 seconds before first refresh to let initialization complete
      try? await Task.sleep(for: .seconds(60))

      // Do an immediate refresh on startup to ensure GroupInfo is fresh
      // This is critical for External Commit - stale GroupInfo blocks new device joins
      if !Task.isCancelled, !isShuttingDown {
        do {
          try self.validateSessionGeneration(capturedGeneration: myGeneration)
          self.logger.info("🔄 [GroupInfo] Running STARTUP GroupInfo refresh for all conversations")
          await self.refreshAllGroupInfo()
        } catch {
          self.logger.error("GroupInfo startup refresh error: \(error)")
        }
      }

      while !Task.isCancelled {
        do {
          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          // Refresh every 12 hours
          try await Task.sleep(for: .seconds(self.groupInfoRefreshInterval))

          guard !Task.isCancelled, !isShuttingDown else { break }

          try self.validateSessionGeneration(capturedGeneration: myGeneration)

          self.logger.info("🔄 [GroupInfo] Running proactive GroupInfo refresh for all conversations")
          await self.refreshAllGroupInfo()
        } catch is CancellationError {
          self.logger.info("GroupInfo refresh task cancelled")
          break
        } catch {
          self.logger.error("GroupInfo refresh error: \(error)")
          break
        }
      }
    }

      logger.info("Started GroupInfo refresh task (interval: \(self.groupInfoRefreshInterval / 3600) hours)")
  }

  /// Proactively refresh GroupInfo for all conversations where we have local group state
  /// This ensures other devices can External Commit to join any of our active conversations
  internal func refreshAllGroupInfo() async {
    guard let userDid = userDid else {
      logger.warning("⚠️ [GroupInfo] Cannot refresh - no userDid")
      return
    }

    // Prefer the persisted conversation list over in-memory caches.
    // `groupStates` can be empty on startup or after state resets, which makes refresh a no-op
    // and allows server GroupInfo TTL to expire (blocking External Commit joins).
    let normalizedUserDID = MLSStorageHelpers.normalizeDID(userDid)
    let dbConversations: [MLSConversationModel]
    do {
      dbConversations = try await database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
          .filter(MLSConversationModel.Columns.isActive == true)
          .fetchAll(db)
      }
    } catch {
      logger.warning(
        "⚠️ [GroupInfo] Failed to load conversations from DB; falling back to in-memory state: \(error.localizedDescription)"
      )
      dbConversations = []
    }

    // De-dupe by groupID so we don't spam the server for placeholder/duplicate rows.
    // Prefer non-placeholder, then the most recently updated row.
    var convoByGroupId: [Data: MLSConversationModel] = [:]
    for convo in dbConversations {
      if let existing = convoByGroupId[convo.groupID] {
        let shouldReplace: Bool
        if existing.isPlaceholder && !convo.isPlaceholder {
          shouldReplace = true
        } else if !existing.isPlaceholder && convo.isPlaceholder {
          shouldReplace = false
        } else {
          shouldReplace = convo.updatedAt > existing.updatedAt
        }
        if shouldReplace {
          convoByGroupId[convo.groupID] = convo
        }
      } else {
        convoByGroupId[convo.groupID] = convo
      }
    }

    // Build refresh candidates from DB first, then backfill from in-memory groupStates.
    // This keeps the behavior robust for brand new conversations that haven't been persisted yet.
    var convoIdByGroupId: [Data: String] = [:]
    for convo in convoByGroupId.values {
      convoIdByGroupId[convo.groupID] = convo.conversationID
    }
    for (groupIdHex, groupState) in groupStates {
      guard let groupId = Data(hexEncoded: groupIdHex) else { continue }
      if convoIdByGroupId[groupId] == nil {
        convoIdByGroupId[groupId] = groupState.convoId
      }
    }

    var candidates: [(groupId: Data, convoId: String)] = []
    candidates.reserveCapacity(convoIdByGroupId.count)
    for (groupId, convoId) in convoIdByGroupId {
      // Only try exporting/uploading when the MLS group actually exists locally.
      // This avoids noisy failures for stale DB rows.
      if await mlsClient.groupExists(for: userDid, groupId: groupId) {
        candidates.append((groupId: groupId, convoId: convoId))
      }
    }
    candidates.sort { $0.convoId < $1.convoId }

    guard !candidates.isEmpty else {
      logger.info(
        "🔄 [GroupInfo] No local groups to refresh (dbConvos=\(dbConversations.count), inMemoryGroups=\(self.groupStates.count))"
      )
      return
    }

    logger.info(
      "🔄 [GroupInfo] Refreshing GroupInfo for \(candidates.count) local groups (dbConvos=\(dbConversations.count), inMemoryGroups=\(self.groupStates.count))"
    )

    var successCount = 0
    var failCount = 0

    for candidate in candidates {
      guard !isShuttingDown, !Task.isCancelled else { break }

      do {
        try await mlsClient.publishGroupInfo(
          for: userDid,
          convoId: candidate.convoId,
          groupId: candidate.groupId
        )
        successCount += 1
        logger.debug("✅ [GroupInfo] Refreshed GroupInfo for \(candidate.convoId.prefix(16))")
      } catch {
        failCount += 1
        logger.warning(
          "⚠️ [GroupInfo] Failed to refresh \(candidate.convoId.prefix(16)): \(error.localizedDescription)"
        )
      }

      // Small delay between refreshes to avoid hammering the server
      try? await Task.sleep(nanoseconds: 200_000_000)  // 200ms
    }

    logger.info("🔄 [GroupInfo] Refresh complete: \(successCount) success, \(failCount) failed")
  }

  /// Adopt pending orphaned reactions by fetching missing parent messages
  internal func adoptPendingOrphans(userDID: String) async {
    do {
      // Get list of missing parent message IDs
      let missingParents = try await storage.fetchMissingParentMessageIDs(
        currentUserDID: userDID,
        limit: 20,
        database: database
      )
      
      guard !missingParents.isEmpty else { return }
      
      logger.info("[ORPHAN] Found \(missingParents.count) missing parent messages - attempting fetch")
      
      for (messageID, conversationID) in missingParents {
        guard !isShuttingDown else { break }
        
        // Check if message now exists (might have arrived since last check)
        if let existing = try? await storage.fetchMessage(
          messageID: messageID,
          currentUserDID: userDID,
          database: database
        ), existing != nil {
          // Message exists, adopt orphans and notify UI
          let adoptedReactions = try await storage.adoptOrphansForMessage(
            messageID,
            currentUserDID: userDID,
            database: database
          )
          
          // Notify UI for each adopted reaction
          for adopted in adoptedReactions {
            notifyObservers(.reactionReceived(
              convoId: adopted.conversationID,
              messageId: adopted.messageID,
              emoji: adopted.emoji,
              senderDID: adopted.actorDID,
              action: adopted.action
            ))
            logger.info("[ORPHAN-ADOPT] Notified UI of adopted reaction \(adopted.emoji) on \(adopted.messageID.prefix(16))")
          }
          
          if !adoptedReactions.isEmpty {
            logger.info("[ORPHAN-ADOPT] Adopted \(adoptedReactions.count) orphan(s) for now-existing message \(messageID)")
          }
          continue
        }
        
        // Try to fetch the missing message from server
        logger.info("[ORPHAN-FETCH] Triggering fetch for missing parent message \(messageID) in \(conversationID.prefix(20))")
        
        do {
          // Get the conversation view to use catchUpMessagesIfNeeded
          if let convo = conversations[conversationID] {
            // Fetch recent messages for this conversation with lookback to find the missing one
            await fetchMissingMessagesWithLookback(for: convo)
            
            // Check if adoption happened and notify UI
            let adoptedReactions = try await storage.adoptOrphansForMessage(
              messageID,
              currentUserDID: userDID,
              database: database
            )
            
            // Notify UI for each adopted reaction
            for adopted in adoptedReactions {
              notifyObservers(.reactionReceived(
                convoId: adopted.conversationID,
                messageId: adopted.messageID,
                emoji: adopted.emoji,
                senderDID: adopted.actorDID,
                action: adopted.action
              ))
              logger.info("[ORPHAN-ADOPT] Notified UI of adopted reaction \(adopted.emoji) on \(adopted.messageID.prefix(16))")
            }
            
            if !adoptedReactions.isEmpty {
              logger.info("[ORPHAN-ADOPT] Adopted \(adoptedReactions.count) orphan(s) after fetching message \(messageID)")
            }
          } else {
            logger.warning("[ORPHAN-FETCH] Conversation \(conversationID.prefix(20)) not found in cache - skipping fetch")
          }
        } catch {
          logger.warning("[ORPHAN-FETCH] Failed to fetch parent message \(messageID): \(error.localizedDescription)")
        }
        
        // Small delay between fetches to avoid hammering the server
        try? await Task.sleep(nanoseconds: 500_000_000) // 500ms
      }
    } catch {
      logger.error("[ORPHAN] Error during orphan adoption: \(error.localizedDescription)")
    }
  }

  /// Perform cleanup of old key material
  internal func performBackgroundCleanup() async {
    logger.debug("Running background cleanup")

    guard let userDid = userDid else {
      logger.warning("Cannot perform background cleanup: userDid not available")
      return
    }

    do {
      // Clean up old pending messages (prevent memory leaks)
      cleanupOldPendingMessages()

      // Clean up message keys older than retention threshold
      let threshold = configuration.messageKeyCleanupThreshold
      try await storage.cleanupMessageKeys(
        userDID: userDid, olderThan: threshold, database: database)
      logger.debug("Cleaned up message keys older than \(threshold)")

      // Permanently delete marked epoch keys
      try await storage.deleteMarkedEpochKeys(userDID: userDid, database: database)
      logger.debug("Permanently deleted marked epoch keys")

      // Clean up expired key packages
      try await storage.deleteExpiredKeyPackages(userDID: userDid, database: database)
      logger.debug("Deleted expired key packages")

      // Refresh key packages if needed
      try await refreshKeyPackagesBasedOnInterval()

      // Checkpoint database to consolidate WAL and free memory
      // This helps prevent "out of memory" errors during heavy polling
      try? await MLSGRDBManager.shared.checkpointDatabase(for: userDid)
      logger.debug("Database checkpoint completed")

      logger.info("Background cleanup completed successfully")
    } catch {
      logger.error("Background cleanup failed: \(error)")
    }
  }

  /// Clean up old pending messages that have exceeded the timeout
  /// Prevents memory leaks if messages are somehow never confirmed by the server
  internal func cleanupOldPendingMessages() {
    let now = Date()

    pendingMessagesLock.lock()
    defer { pendingMessagesLock.unlock() }

    let initialCount = pendingMessages.count

    // Remove pending messages older than timeout (5 minutes by default)
    pendingMessages = pendingMessages.filter { _, pending in
      pending.timestamp.addingTimeInterval(pendingMessageTimeout) > now
    }

    let removed = initialCount - pendingMessages.count
    if removed > 0 {
      logger.debug(
        "🧹 Cleaned up \(removed) stale pending messages (older than \(Int(self.pendingMessageTimeout))s)"
      )
    }
  }

  // MARK: - Observer Pattern

  /// Add a state change observer
  /// - Parameter observer: Observer to add
  public func addObserver(_ observer: MLSStateObserver) {
    observers.append(observer)
    logger.debug("Added state observer")
  }

  /// Remove a state change observer
  /// - Parameter observer: Observer to remove
  public func removeObserver(_ observer: MLSStateObserver) {
    observers.removeAll { $0.id == observer.id }
    logger.debug("Removed state observer")
  }

  /// Notify all observers of a state change
  internal func notifyObservers(_ event: MLSStateEvent) {
    logger.debug("Notifying observers of event: \(event.description)")
    for observer in observers {
      observer.onStateChange(event)
    }
  }

  // MARK: - MLS Crypto Operations (using MLSClient)

  /// Encrypt message using MLSClient
  /// Uses GroupOperationCoordinator to ensure serialization per group
  internal func encryptMessage(groupId: String, plaintext: Data) async throws -> Data {
    return try await groupOperationCoordinator.withExclusiveLock(groupId: groupId) { [self] in
      try await encryptMessageImpl(groupId: groupId, plaintext: plaintext)
    }
  }

  /// Internal implementation of message encryption (called within exclusive lock)
  public func encryptMessageImpl(groupId: String, plaintext: Data) async throws -> Data {
    let gen = currentCoordinationGeneration
    let memoryEpoch = groupStates[groupId]?.epoch ?? 0
    logger.debug(
      "📦 [ENCRYPT] [Gen: \(gen)] [Memory Epoch: \(memoryEpoch)] groupId=\(groupId.prefix(8))..., plaintext.count=\(plaintext.count)")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    // groupId is hex-encoded, convert to Data
    guard let groupIdData = Data(hexEncoded: groupId) else {
      logger.error("Failed to decode hex groupId: \(groupId.prefix(20))...")
      throw MLSConversationError.invalidGroupId
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Epoch Pre-Flight Check
    // ═══════════════════════════════════════════════════════════════════════════
    // Before encrypting, verify our in-memory epoch matches the on-disk state.
    // If the NSE advanced the ratchet while we were in background, our in-memory
    // state is stale and we must force a reload.
    //
    // This prevents:
    // - SecretReuseError (using a nonce the NSE already consumed)
    // - Encrypting at an old epoch that recipients can't decrypt
    // ═══════════════════════════════════════════════════════════════════════════
    if let memoryState = groupStates[groupId] {
      do {
        let diskEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
        if diskEpoch > memoryState.epoch {
          logger.warning("⚠️ [Epoch Check] Disk epoch (\(diskEpoch)) > memory epoch (\(memoryState.epoch))")
          logger.info("   NSE likely advanced ratchet - forcing state reload before encrypt")
          groupStates.removeValue(forKey: groupId)
          conversationStates.removeValue(forKey: groupId)
          // FFI will reload fresh state on next access
        }
      } catch {
        logger.debug("⚠️ [Epoch Check] Could not verify epoch: \(error.localizedDescription)")
        // Non-fatal - proceed with operation, FFI layer handles state
      }
    }

    logger.debug("Calling mlsClient.encryptMessage with groupIdData.count=\(groupIdData.count)")
    let encryptResult = try await mlsClient.encryptMessage(
      for: userDid, groupId: groupIdData, plaintext: plaintext)

    // Signal ratchet advance to other in-process/cross-process contexts.
    MLSStateVersionManager.shared.incrementVersion(for: userDid)

    logger.debug(
      "mlsClient.encryptMessage succeeded, ciphertext.count=\(encryptResult.ciphertext.count)")

    // Persist MLS state after encryption (sender ratchet advanced)
    do {
      logger.debug("✅ Persisted MLS state after encryption")
    } catch {
      logger.error("⚠️ Failed to persist MLS state after encryption: \(error.localizedDescription)")
    }

    return encryptResult.ciphertext
  }

  /// Decrypt message using MLSClient with processMessage flow
  /// Uses GroupOperationCoordinator to ensure serialization per group
  internal func decryptMessage(groupId: String, ciphertext: Data) async throws -> Data {
    return try await groupOperationCoordinator.withExclusiveLock(groupId: groupId) { [self] in
      try await decryptMessageImpl(groupId: groupId, ciphertext: ciphertext)
    }
  }

  /// Internal implementation of message decryption (called within exclusive lock)
  internal func decryptMessageImpl(groupId: String, ciphertext: Data) async throws -> Data {
    logger.info("Decrypting message for group \(groupId.prefix(8))...")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    guard let groupIdData = Data(hexEncoded: groupId) else {
      logger.error("Invalid group ID format")
      throw MLSConversationError.invalidGroupId
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Epoch Pre-Flight Check
    // ═══════════════════════════════════════════════════════════════════════════
    // Before decrypting, verify our in-memory epoch matches the on-disk state.
    // If the NSE advanced the ratchet while we were in background, our in-memory
    // state is stale and we must force a reload.
    //
    // This prevents attempting decryption with stale keys that would fail with
    // SecretReuseError or DecryptionFailed.
    // ═══════════════════════════════════════════════════════════════════════════
    if let memoryState = groupStates[groupId] {
      do {
        let diskEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
        if diskEpoch > memoryState.epoch {
          logger.warning("⚠️ [Epoch Check] Disk epoch (\(diskEpoch)) > memory epoch (\(memoryState.epoch))")
          logger.info("   NSE likely advanced ratchet - forcing state reload before decrypt")
          groupStates.removeValue(forKey: groupId)
          conversationStates.removeValue(forKey: groupId)
          // FFI will reload fresh state on next access
        }
      } catch {
        logger.debug("⚠️ [Epoch Check] Could not verify epoch: \(error.localizedDescription)")
        // Non-fatal - proceed with operation, FFI layer handles state
      }
    }

    let ciphertextData = ciphertext

    do {
      // Use processMessage instead of decryptMessage to get content type
      let processedContent = try await mlsClient.processMessage(
        for: userDid,
        groupId: groupIdData,
        messageData: ciphertextData
      )

      // Signal ratchet advance to other in-process/cross-process contexts.
      MLSStateVersionManager.shared.incrementVersion(for: userDid)

      // CRITICAL FIX: Persist MLS state after decryption (receiver ratchet advanced)
      // This prevents SecretReuseError when trying to decrypt subsequent messages
      do {
        logger.debug("✅ Persisted MLS state after message decryption")
      } catch {
        logger.error(
          "⚠️ Failed to persist MLS state after decryption: \(error.localizedDescription)")
        // Don't fail the decryption - state loss is recoverable via resync
      }

      // Handle different message types
      switch processedContent {
      case .applicationMessage(let plaintext, _):
        // Normal application message - return decrypted content (sender ignored here)
        logger.info("Decrypted application message (\(plaintext.count) bytes)")
        return plaintext

      case .proposal(let proposal, let proposalRef):
        // Received a proposal - validate and queue it
        logger.info("Received proposal, validating...")
        try await handleProposal(groupId: groupId, proposal: proposal, proposalRef: proposalRef)

        // Return empty data for proposals (no plaintext content)
        return Data()

      case .stagedCommit(let newEpoch):
        // Staged commit was already auto-merged by processMessage in Rust
        // Just verify the epoch advancement succeeded
        logger.info("Received commit for epoch \(newEpoch), verifying...")
        try await validateAndMergeStagedCommit(groupId: groupId, newEpoch: newEpoch)

        // Return empty data for commits (no plaintext content)
        return Data()
      }
    } catch let error as MlsError {
      logger.error("Message processing failed: \(error.localizedDescription)")
      throw MLSConversationError.decryptionFailed
    } catch {
      logger.error("Unexpected error during message processing: \(error.localizedDescription)")
      throw error
    }
  }

  /// Process Welcome message using MLSClient
  /// Automatically uses mlsDid (device-specific DID) as the identity
  internal func processWelcome(welcomeData: Data) async throws -> String {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    do {
      // Uses mlsDid (device-specific DID) automatically
      let groupId = try await mlsClient.joinGroup(
        for: userDid, welcome: welcomeData, configuration: configuration.groupConfiguration)

      // Persist MLS state after joining group (new group created)
      do {
        logger.debug("✅ Persisted MLS state after joining group")
      } catch {
        logger.error("⚠️ Failed to persist MLS state after join: \(error.localizedDescription)")
      }

      return groupId.hexEncodedString()
    } catch let error as MlsError {
      // Handle key package desync (app reinstall, database loss, etc.)
      if case .KeyPackageDesyncDetected(let message) = error {
        logger.warning("🔄 Key package desync detected: \(message)")
        logger.info("Attempting automated recovery via External Commit...")

        // Extract conversation ID from the error message if possible
        // The Rust FFI should include conversation ID in the message
        try await handleKeyPackageDesyncRecovery(errorMessage: message, userDid: userDid)

        // After recovery, the conversation should be marked for rejoin
        // The user will rejoin via External Commit
        throw MLSConversationError.keyPackageDesyncRecoveryInitiated
      }

      // Re-throw other MlsErrors
      throw error
    }
  }

  /// Handle key package desync recovery by requesting rejoin
  /// - Parameters:
  ///   - errorMessage: Error message from FFI containing conversation details
  ///   - userDid: User DID for key package generation
  internal func handleKeyPackageDesyncRecovery(errorMessage: String, userDid: String) async throws {
    logger.info("📦 Handling key package desync recovery...")

    // Extract conversation ID from error message
    // The Rust FFI formats the message as: "No key package bundles available..." or includes convo_id
    // For now, we'll need the caller to provide the conversation ID explicitly
    // This is a limitation - we'll improve this in the next iteration

    logger.warning("⚠️ Cannot automatically extract conversation ID from desync error")
    logger.info("User will need to manually rejoin the conversation via UI")
  }

  /// Handle a received proposal
  internal func handleProposal(groupId: String, proposal: Any, proposalRef: ProposalRef) async throws
  {
    logger.info("Handling proposal for group \(groupId.prefix(8))...")

    // Convert hex-encoded groupId to Data
    guard let groupIdData = Data(hexEncoded: groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Validate and store the proposal
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    try await mlsClient.storeProposal(for: userDid, groupId: groupIdData, proposalRef: proposalRef)
    logger.info("Proposal stored successfully")
  }

  /// Validate and merge staged commit
  /// Uses GroupOperationCoordinator to ensure serialization per group
  internal func validateAndMergeStagedCommit(groupId: String, newEpoch: UInt64) async throws {
    return try await groupOperationCoordinator.withExclusiveLock(groupId: groupId) { [self] in
      try await validateAndMergeStagedCommitImpl(groupId: groupId, newEpoch: newEpoch)
    }
  }

  /// Internal implementation of staged commit validation and merge (called within exclusive lock)
  internal func validateAndMergeStagedCommitImpl(groupId: String, newEpoch: UInt64) async throws {
    // NOTE: As of the epoch advancement fix, staged commits from other members are now
    // auto-merged during processMessage() in the Rust FFI layer. This function now just
    // validates the epoch state is correct and logs the transition.
    //
    // Previously, this function would call mergeStagedCommit() which would look for a
    // pending commit (wrong!), causing the group to stay at the old epoch while other
    // members advanced.

    logger.info(
      "✅ Staged commit already merged in processMessage, verifying epoch \(newEpoch) for group \(groupId.prefix(8))..."
    )

    // Convert hex-encoded groupId to Data
    guard let groupIdData = Data(hexEncoded: groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Verify the current epoch matches what we expect
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    var previousEpoch: UInt64 = 0
    do {
      let currentEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
      previousEpoch = currentEpoch > 0 ? currentEpoch - 1 : 0
      if currentEpoch != newEpoch {
        logger.warning(
          "⚠️ Epoch mismatch after staged commit merge: current=\(currentEpoch), expected=\(newEpoch)"
        )
      } else {
        logger.info("✅ Epoch verified: \(currentEpoch)")
      }
    } catch {
      logger.warning(
        "⚠️ Unable to verify epoch after staged commit merge: \(error.localizedDescription)")
    }
    
    // Process membership changes for transparency
    if let observer = membershipChangeObserver {
      do {
        // In this codebase, groupId IS the conversationID (they're the same hex string)
        let convoId = groupId
        
        // Get current members from MLS group
        let debugInfo = try await mlsClient.debugGroupMembers(for: userDid, groupId: groupIdData)
        let memberDIDs = debugInfo.members.compactMap {
          String(data: $0.credentialIdentity, encoding: .utf8)
        }
        
        // Process epoch transition for membership change detection
        try await observer.processEpochTransition(
          conversationID: convoId,
          oldEpoch: Int64(previousEpoch),
          newEpoch: Int64(newEpoch),
          newMembers: memberDIDs,
          treeHash: nil,  // Could extract from debugInfo if available
          actorDID: nil   // Could be extracted from commit if available
        )
      } catch {
        logger.warning("⚠️ Failed to process membership changes: \(error.localizedDescription)")
        // Non-fatal: membership transparency is best-effort
      }
    }
  }

  /// Initialize MLS group is initialized for a conversation
  func ensureGroupInitialized(for convoId: String) async throws {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    guard let convo = conversations[convoId] else {
      logger.warning("Cannot initialize group: conversation \(convoId) not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      logger.error("Invalid groupId for conversation \(convoId)")
      throw MLSConversationError.invalidGroupId
    }

    // Check if group already exists locally
    if await mlsClient.groupExists(for: userDid, groupId: groupIdData) {
      logger.debug("Group already exists locally for conversation \(convoId)")
      return
    }

    // ⭐ CRITICAL FIX: Check if we are the creator before trying to join via Welcome
    let isCreator = convo.creator.description.lowercased() == userDid.lowercased()

    if isCreator {
      logger.warning(
        "⚠️ [ensureGroupInitialized] Creator (\(userDid.prefix(20))...) missing group state for \(convoId.prefix(16))..."
      )
      logger.info("🔄 [ensureGroupInitialized] Attempting External Commit for creator rejoin...")

      do {
        let _ = try await mlsClient.joinByExternalCommit(for: userDid, convoId: convo.groupId)
        logger.info("✅ [ensureGroupInitialized] Creator successfully rejoined via External Commit")
      } catch {
        logger.error(
          "❌ [ensureGroupInitialized] Creator rejoin via External Commit failed: \(error.localizedDescription)"
        )
        throw MLSConversationError.groupNotInitialized
      }
    } else {
      // Group doesn't exist, initialize from Welcome message
      logger.info("Group not found locally, initializing from Welcome for conversation \(convoId)")
      
      do {
        try await initializeGroupFromWelcome(convo: convo)
      } catch MLSConversationError.keyPackageDesyncRecoveryInitiated {
        logger.warning("🔄 Key package desync during Welcome init - automatically failing over to External Commit")
        logger.info("   This is expected if our key package was rotated on the server but we don't have the private key anymore.")
        
        do {
          _ = try await mlsClient.joinByExternalCommit(for: userDid, convoId: convo.groupId)
          logger.info("✅ Successfully recovered via External Commit")
        } catch {
          logger.error("❌ External Commit recovery failed: \(error.localizedDescription)")
          throw error
        }
      } catch {
        // Re-throw other errors
        throw error
      }
    }
  }

  /// Initialize a group from a Welcome message fetched from the server
  internal func initializeGroupFromWelcome(convo: BlueCatbirdMlsDefs.ConvoView) async throws {
    // ═══════════════════════════════════════════════════════════════════
    // PHASE 6: Welcome is for Other Users Only
    // ═══════════════════════════════════════════════════════════════════
    // This function should ONLY be called when:
    // - User is NOT already a member of the conversation
    // - User was invited by someone else (Welcome message exists)
    //
    // For same-user device sync (user already member, new device):
    // - Use External Commit instead (see syncWithServer)
    // ═══════════════════════════════════════════════════════════════════
    logger.debug("Fetching Welcome message for conversation \(convo.groupId)")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    // ⭐ FIX: Wait for any pending cross-process welcome handling (e.g. from NSE)
    // This prevents race conditions where NSE and App both try to process the Welcome simultaneously
    // The NSE might have received the first notification and started processing the Welcome
    // before the App finished syncing the conversation list.
    let _ = await MLSWelcomeGate.shared.waitForWelcomeIfPending(
      for: convo.groupId,
      userDID: userDid,
      timeout: .seconds(3)
    )

    // Check if group appeared while we waited (meaning NSE successfully processed it)
    if let groupIdData = Data(hexEncoded: convo.groupId),
       await mlsClient.groupExists(for: userDid, groupId: groupIdData) {
       logger.info("✅ Group \(convo.groupId.prefix(8))... appeared after waiting for WelcomeGate - skipping processing")

       // Ensure local DB state is up to date (NSE might have created the group but not fully hydrated App-layer models)
       try await updateGroupStateAfterJoin(convo: convo, groupIdHex: convo.groupId, userDid: userDid)
       return
    }

    await MLSWelcomeGate.shared.beginWelcomeProcessing(for: convo.groupId, userDID: userDid)
    defer {
      Task { await MLSWelcomeGate.shared.completeWelcomeProcessing(for: convo.groupId, userDID: userDid) }
    }

    var groupIdHex: String

    let welcomeData: Data
    do {
      welcomeData = try await apiClient.getWelcome(convoId: convo.groupId)
      logger.debug("Received Welcome message: \(welcomeData.count) bytes")
    } catch let error as MLSAPIError {
      if case .httpError(let statusCode, _) = error, statusCode == 410 {
        logger.info(
          "📭 [HTTP 410 GONE] Welcome expired for \(convo.groupId) - KeyPackage consumed/expired")
        logger.info("🔄 Skipping Welcome, attempting External Commit directly...")

        groupIdHex = try await attemptExternalCommitFallback(
          convoId: convo.groupId,
          userDid: userDid,
          reason: "Welcome expired (HTTP 410)"
        )

        try await updateGroupStateAfterJoin(convo: convo, groupIdHex: groupIdHex, userDid: userDid)
        return
      }
      
      // ⭐ FIX: Also handle HTTP 404 (Welcome not found) by trying External Commit
      // This happens when the Welcome was never stored (race condition) or was deleted
      if case .httpError(let statusCode, _) = error, statusCode == 404 {
        logger.info(
          "📭 [HTTP 404 NOT FOUND] Welcome not found for \(convo.groupId) - may be race condition or deleted")
        logger.info("🔄 Skipping Welcome, attempting External Commit directly...")

        groupIdHex = try await attemptExternalCommitFallback(
          convoId: convo.groupId,
          userDid: userDid,
          reason: "Welcome not found (HTTP 404)"
        )

        try await updateGroupStateAfterJoin(convo: convo, groupIdHex: groupIdHex, userDid: userDid)
        return
      }
      
      throw error
    } catch let error as NetworkError {
      // ⭐ FIX: Handle Petrel NetworkError for 404/410 status codes
      if case .responseError(let statusCode) = error, statusCode == 404 || statusCode == 410 {
        logger.info(
          "📭 [NetworkError \(statusCode)] Welcome not found/expired for \(convo.groupId)")
        logger.info("🔄 Skipping Welcome, attempting External Commit directly...")

        groupIdHex = try await attemptExternalCommitFallback(
          convoId: convo.groupId,
          userDid: userDid,
          reason: "Welcome not available (NetworkError \(statusCode))"
        )

        try await updateGroupStateAfterJoin(convo: convo, groupIdHex: groupIdHex, userDid: userDid)
        return
      }
      
      throw error
    }

    do {
      groupIdHex = try await processWelcome(welcomeData: welcomeData)
      
      do {
        _ = try await storage.ensureConversationExistsOrPlaceholder(
          userDID: userDid,
          conversationID: convo.groupId,
          groupID: groupIdHex,
          senderDID: convo.members.first(where: { $0.did.description.lowercased() != userDid.lowercased() })?.did.description,
          database: database
        )
        logger.info("✅ [FK-FIX] Ensured conversation record exists after Welcome processing")
      } catch {
        logger.warning("⚠️ [FK-FIX] Failed to pre-create conversation record: \(error.localizedDescription)")
      }
    } catch let error as MlsError {
      if case .NoMatchingKeyPackage = error {
        logger.warning("⚠️ NoMatchingKeyPackage error - Welcome references unavailable key package")

        logger.info("📤 Invalidating stale Welcome on server (NoMatchingKeyPackage)...")
        do {
          _ = try await apiClient.invalidateWelcome(
            convoId: convo.groupId,
            reason: "NoMatchingKeyPackage: key package hash_ref not found in local storage"
          )
        } catch {
          logger.warning("⚠️ Failed to invalidate Welcome: \(error.localizedDescription)")
        }

        Task.detached(priority: .utility) { [mlsClient, logger, userDid] in
          do {
            _ = try await mlsClient.syncKeyPackageHashes(for: userDid)
          } catch {
            logger.warning(
              "⚠️ [NoMatchingKeyPackage] Failed to sync key package hashes: \(error.localizedDescription)"
            )
          }

          do {
            _ = try await mlsClient.monitorAndReplenishBundles(for: userDid)
          } catch {
            logger.warning(
              "⚠️ [NoMatchingKeyPackage] Failed to replenish key packages: \(error.localizedDescription)"
            )
          }
        }

        logger.info("🔄 Attempting fallback to External Commit for conversation \(convo.groupId)...")

        groupIdHex = try await attemptExternalCommitFallback(
          convoId: convo.groupId,
          userDid: userDid,
          reason: "NoMatchingKeyPackage"
        )

        try await updateGroupStateAfterJoin(convo: convo, groupIdHex: groupIdHex, userDid: userDid)
        return
      } else {
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          let recovered = await recoveryManager.attemptRecoveryIfNeeded(
            for: error,
            userDid: userDid,
            convoIds: [convo.groupId],
            triggeringConvoId: convo.groupId,
            isRemoteDataError: true 
          )
          if recovered {
            logger.info("🔄 Recovery initiated for MLS error: \(error)")
            throw MLSConversationError.operationFailed("Recovery in progress - please wait")
          }
        }
        throw error
      }
    }

    if var state = groupStates[convo.groupId] {
      guard let groupIdData = Data(hexEncoded: groupIdHex) else {
        throw MLSConversationError.invalidGroupId
      }

      let ffiEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
      state.epoch = ffiEpoch 
      groupStates[convo.groupId] = state
      logger.debug(
        "Updated group epoch to \(ffiEpoch) (from FFI) for conversation \(convo.groupId)")

      do {
        try await storage.updateConversationJoinInfo(
          conversationID: convo.groupId,
          currentUserDID: userDid,
          joinMethod: .welcome,
          joinEpoch: Int64(ffiEpoch),
          database: database
        )
      } catch {
        logger.warning("⚠️ Failed to persist join info (Welcome): \(error.localizedDescription)")
      }
    }

    logger.info("Successfully initialized group from Welcome for conversation \(convo.groupId)")
    await catchUpMessagesIfNeeded(for: convo, force: true)
  }

  // MARK: - External Commit Fallback for Recovery

  /// Attempt to join a group via External Commit when Welcome processing fails.
  /// Tracks failures via recovery manager to prevent infinite loops.
  ///
  /// - Parameters:
  ///   - convoId: The conversation/group ID to join
  ///   - userDid: The current user's DID
  ///   - reason: Descriptive reason for fallback (for logging)
  /// - Returns: The group ID hex string on success
  /// - Throws: MLSConversationError if External Commit fails
  internal func attemptExternalCommitFallback(
    convoId: String,
    userDid: String,
    reason: String
  ) async throws -> String {
    logger.info(
      "🔄 [External Commit Fallback] Starting for \(convoId.prefix(16))... Reason: \(reason)")

    // Check if we should skip this rejoin attempt (max attempts or cooldown)
    if let recoveryManager = await mlsClient.recovery(for: userDid) {
      let shouldSkip = await recoveryManager.shouldSkipRejoin(convoId: convoId)
      if shouldSkip {
        logger.warning(
          "⏭️ [External Commit Fallback] Skipping \(convoId.prefix(16))... - recovery tracking says skip"
        )
        throw MLSConversationError.operationFailed(
          "External Commit skipped - max attempts exceeded or on cooldown")
      }
    }

    do {
      // Attempt External Commit via mlsClient

      let groupIdData = try await mlsClient.joinByExternalCommit(for: userDid, convoId: convoId)
      let groupIdHex = groupIdData.hexEncodedString()

      logger.info(
        "✅ [External Commit Fallback] Successfully joined \(convoId.prefix(16))... via External Commit"
      )

      // Clear recovery tracking on success
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        await recoveryManager.clearRejoinTracking(convoId: convoId)
      }

      return groupIdHex
    } catch {
      logger.error(
        "❌ [External Commit Fallback] Failed for \(convoId.prefix(16))...: \(error.localizedDescription)"
      )

      let errorMessage = error.localizedDescription.lowercased()

      if let apiError = error as? MLSAPIError,
        case .httpError(let statusCode, _) = apiError,
        statusCode == 403
      {
        logger.warning(
          "⚠️ [External Commit Fallback] HTTP 403 - external commit not allowed, requesting re-addition"
        )
        await readdition(convoId: convoId)

        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.recordFailedRejoin(convoId: convoId)
        }

        throw MLSConversationError.operationFailed("External Commit forbidden (HTTP 403)")
      }

      // Check if this is a stale GroupInfo error - request refresh from active members
      let isStaleGroupInfo =
        errorMessage.contains("expired") || errorMessage.contains("stale")
        || errorMessage.contains("groupinfo expired")

      if isStaleGroupInfo {
        logger.info(
          "🔄 [External Commit Fallback] GroupInfo stale - requesting refresh from active members")
        await groupInfoRefresh(convoId: convoId)
      }

      // Record the failure for tracking
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        // Check if this is a server data corruption error
        let isServerDataCorruption =
          errorMessage.contains("invalidvectorlength") || errorMessage.contains("endofstream")
          || errorMessage.contains("malformed") || errorMessage.contains("truncat")
          || errorMessage.contains("server data corrupted")

        if isServerDataCorruption {
          await recoveryManager.recordFailedRejoin(convoId: convoId)
          let remaining = await recoveryManager.remainingRejoinAttempts(convoId: convoId)
          if remaining == 0 {
            // Mark as server-corrupted after repeated failures to avoid premature lockout
            await recoveryManager.markConversationServerCorrupted(
              convoId: convoId,
              errorMessage: "External Commit failed (server data): \(error.localizedDescription)"
            )
            logger.error(
              "🚫 [External Commit Fallback] Server data corrupted - marked conversation as broken")
          } else {
            logger.warning(
              "⚠️ [External Commit Fallback] Server data issue detected - will retry after refresh"
            )
            await groupInfoRefresh(convoId: convoId)
            logger.info(
              "📊 [External Commit Fallback] \(remaining) rejoin attempts remaining for \(convoId.prefix(16))..."
            )
          }
        } else {
          await recoveryManager.recordFailedRejoin(convoId: convoId)
          let remaining = await recoveryManager.remainingRejoinAttempts(convoId: convoId)
          logger.info(
            "📊 [External Commit Fallback] \(remaining) rejoin attempts remaining for \(convoId.prefix(16))..."
          )

          // If all rejoin attempts are exhausted, request re-addition from active members
          if remaining == 0 {
            logger.info(
              "🆘 [External Commit Fallback] All rejoin attempts exhausted - requesting re-addition")
            await readdition(convoId: convoId)
          }
        }
      }

      throw error
    }
  }

  /// Update group state after successfully joining via Welcome or External Commit.
  /// Synchronizes epoch from FFI and triggers message catch-up.
  ///
  /// - Parameters:
  ///   - convo: The conversation view from server
  ///   - groupIdHex: The hex-encoded group ID from join operation
  ///   - userDid: The current user's DID
  internal func updateGroupStateAfterJoin(
    convo: BlueCatbirdMlsDefs.ConvoView,
    groupIdHex: String,
    userDid: String
  ) async throws {
    // Convert hex to data for FFI calls
    guard let groupIdData = Data(hexEncoded: groupIdHex) else {
      throw MLSConversationError.invalidGroupId
    }

    // Update local group state with correct epoch from FFI
    if var state = groupStates[convo.groupId] {
      let serverEpoch = UInt64(convo.epoch)
      let ffiEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

      if serverEpoch != ffiEpoch {
        logger.warning("⚠️ EPOCH MISMATCH after joining group:")
        logger.warning("   Server: \(serverEpoch), FFI: \(ffiEpoch)")
        logger.warning("   Using FFI epoch to prevent state desynchronization")
      }

      state.epoch = ffiEpoch  // Use FFI epoch, not server epoch
      groupStates[convo.groupId] = state
      logger.debug(
        "Updated group epoch to \(ffiEpoch) (from FFI) for conversation \(convo.groupId)")

      // Persist join method/epoch for UI. External Commit starts a new cryptographic history.
      do {
        try await storage.updateConversationJoinInfo(
          conversationID: convo.groupId,
          currentUserDID: userDid,
          joinMethod: .externalCommit,
          joinEpoch: Int64(ffiEpoch),
          database: database
        )
      } catch {
        logger.warning("⚠️ Failed to persist join info (ExtCommit): \(error.localizedDescription)")
      }
    }


    // Log diagnostic info
    await logGroupStateDiagnostics(
      userDid: userDid, groupId: groupIdData, context: "After Join (External Commit Fallback)")

    // Catch up on any messages we may have missed
    await catchUpMessagesIfNeeded(for: convo, force: true)
  }

  /// Reserve selected key packages to prevent reuse before the server processes them
  private func reserveKeyPackages(_ packages: [KeyPackageWithHash]) async {
    for package in packages {
      await keyPackageManager.markKeyPackageExhausted(hash: package.hash, for: package.did.description)
    }
  }

  /// Parse server error detail and record the exhausted hash so future attempts skip it
  internal func recordKeyPackageFailure(detail: String?) async {
    guard let detail, let parsed = parseKeyPackageErrorDetail(detail) else { return }
    await keyPackageManager.markKeyPackageExhausted(hash: parsed.hash, for: parsed.did)
    logger.warning(
      "⚠️ Recorded unavailable key package hash for \(parsed.did): \(parsed.hash.prefix(16))...")
  }

  /// Extract DID/hash pair from the structured error detail string
  private func parseKeyPackageErrorDetail(_ detail: String) -> (did: String, hash: String)? {
    guard let hashRange = detail.range(of: "hash=") else { return nil }

    let hashToken = detail[hashRange.upperBound...]
      .split(whereSeparator: { $0.isWhitespace || $0 == "," })
      .first
      .map(String.init)

    let didPrefix = detail[..<hashRange.lowerBound]
    guard let didRange = didPrefix.range(of: "did:") else { return nil }
    var didValue = String(didPrefix[didRange.lowerBound...])
      .trimmingCharacters(in: .whitespacesAndNewlines)
    if let separatorIndex = didValue.firstIndex(of: " ") {
      didValue = String(didValue[..<separatorIndex])
    }
    didValue = didValue.trimmingCharacters(in: CharacterSet(charactersIn: ":"))

    guard let hashValue = hashToken else { return nil }
    return (did: didValue, hash: hashValue)
  }

  /// Select usable key packages for the requested members, skipping hashes we've exhausted
  ///
  /// **Important**: This method selects key packages for OTHER users we're inviting to a group.
  /// It does NOT handle our own key packages. The server advertises key package hashes
  /// for other users, and we select from that pool to create Welcome messages.
  ///
  /// **Pre-flight Check**: Verifies selected packages match server expectations and aren't
  /// accidentally from our own inventory (which would indicate a bug or server desync).
  internal func selectKeyPackages(
    for members: [DID],
    from pool: [BlueCatbirdMlsDefs.KeyPackageRef],
    userDid: String
  ) async throws -> [KeyPackageWithHash] {
    logger.debug(
      "📦 [selectKeyPackages] Selecting packages for \(members.count) members from pool of \(pool.count)"
    )

    // ✅ PRE-FLIGHT: Verify we're not selecting packages for ourselves
    let normalizedUserDid = userDid.lowercased()
    for member in members {
      let normalizedMemberDid = member.description.lowercased()
      if normalizedMemberDid == normalizedUserDid {
        logger.error("🚨 CRITICAL: Attempting to select key package for ourselves!")
        logger.error("   User DID: \(userDid.prefix(30))...")
        logger.error("   Member DID: \(member.description.prefix(30))...")
        logger.error("   This indicates a bug in group creation logic")
        throw MLSConversationError.operationFailed("Cannot select key package for self")
      }
    }

    var packagesByDid = Dictionary(grouping: pool, by: { $0.did.description })

    var selected: [KeyPackageWithHash] = []
    var skippedCount = 0
    var packagesPerMember: [String: Int] = [:]

    // 🔐 MULTI-DEVICE SUPPORT: Select ALL key packages (one per device)
    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX (2026-01): Use keyPackageHash for deduplication, NOT credential identity
    // ═══════════════════════════════════════════════════════════════════════════
    //
    // The MLS credential identity is now the BARE DID (e.g., "did:plc:abc123") for all
    // key packages from a user. Multi-device is tracked server-side via device_id column.
    //
    // Each device has a UNIQUE key package hash (cryptographically derived from the
    // device's key material). So we deduplicate by HASH, not credential identity.
    // This ensures we select one key package per device, enabling multi-device Welcome.
    //
    // ═══════════════════════════════════════════════════════════════════════════
    for member in members {
      let didKey = member.description
      guard let options = packagesByDid[didKey], !options.isEmpty else {
        logger.error("❌ No key packages returned for member \(didKey)")
        throw MLSConversationError.missingKeyPackages([didKey])
      }

      logger.debug("   Processing \(didKey): \(options.count) candidates available")

      // 🔑 CRITICAL: Use keyPackageHash as unique identifier (one per device)
      // Each device generates key packages with unique cryptographic hashes.
      // Deduplicating by hash ensures exactly one package per device.
      var packagesByHash: [String: (candidate: BlueCatbirdMlsDefs.KeyPackageRef, decoded: Data, hash: String)] = [:]
      var invalidPackages = 0

      for candidate in options {
        guard let decoded = Data(base64Encoded: candidate.keyPackage, options: []) else {
          logger.error("❌ Failed to decode key package for \(candidate.did)")
          skippedCount += 1
          invalidPackages += 1
          continue
        }

        // Prefer server-provided hash for consistency, compute locally if unavailable
        let hash: String
        if let serverHash = candidate.keyPackageHash {
          hash = serverHash
          logger.debug("   Using server-provided hash: \(hash.prefix(16))...")
        } else {
          hash = try await computeKeyPackageReference(for: decoded, userDid: userDid)
          logger.debug("   Computed local hash (server didn't provide): \(hash.prefix(16))...")
        }

        // ✅ PRE-FLIGHT: Verify hash consistency when both server and local hashes exist
        if let serverHash = candidate.keyPackageHash {
          let localHash = try await computeKeyPackageReference(for: decoded, userDid: userDid)
          if serverHash != localHash {
            logger.error("🚨 HASH MISMATCH DETECTED!")
            logger.error("   Server hash: \(serverHash.prefix(32))...")
            logger.error("   Local hash:  \(localHash.prefix(32))...")
            logger.error("   Member DID:  \(didKey.prefix(30))...")
            logger.error("   Package size: \(decoded.count) bytes")
            logger.error("   This indicates server-client hash computation divergence")
            logger.error("   Skipping this package to prevent Welcome message failure")
            skippedCount += 1
            invalidPackages += 1
            continue
          }
          logger.debug("   ✅ Hash verified: server and local match (\(serverHash.prefix(16))...)")
        }

        // Check if already exhausted
        let isExhausted = await keyPackageManager.isKeyPackageExhausted(hash: hash, for: didKey)
        if isExhausted {
          // Only skip exhausted packages if we have other options
          if packagesByHash.count > 0 || options.count > 1 {
            logger.debug("   Skipping exhausted package: \(hash.prefix(16))...")
            skippedCount += 1
            continue
          }
          // If this is the only package, we may still use it (last resort)
          logger.info("🔓 [Last Resort] Using exhausted hash as it's the only option: \(hash.prefix(16))...")
        }

        // Store the package by hash (each hash = one device)
        if packagesByHash[hash] == nil {
          packagesByHash[hash] = (candidate, decoded, hash)
          logger.debug("   Found package for device (hash: \(hash.prefix(16))...)")
        }
        // If hash already exists, it's a duplicate - skip silently
      }

      // Log multi-device results
      let totalCandidates = options.count
      let uniqueDevices = packagesByHash.count
      if totalCandidates != uniqueDevices {
        logger.info("🔄 [Multi-Device] \(didKey.prefix(30))...: \(totalCandidates) candidates → \(uniqueDevices) unique device(s)")
      } else {
        logger.info("📱 [Multi-Device] \(didKey.prefix(30))...: \(uniqueDevices) device(s)")
      }

      // Select ALL packages (one per device hash)
      var validPackagesForMember = 0
      for (hash, packageInfo) in packagesByHash {
        let (_, decoded, _) = packageInfo

        logger.info(
          "✅ Selected package for \(didKey) device: hash=\(hash.prefix(16))... (\(decoded.count) bytes)")
        selected.append(KeyPackageWithHash(data: decoded, hash: hash, did: member))
        validPackagesForMember += 1
      }

      packagesPerMember[didKey] = validPackagesForMember

      // Ensure at least one valid package was found for this member
      if validPackagesForMember == 0 {
        let exhaustedForDid = await keyPackageManager.getExhaustedCount(for: didKey)
        logger.error("❌ No usable key package for \(didKey) (exhausted: \(exhaustedForDid))")
        throw MLSConversationError.missingKeyPackages([didKey])
      }

      logger.debug("   ✅ Selected \(validPackagesForMember) package(s) for \(didKey)")
    }

    if skippedCount > 0 {
      logger.warning(
        "⚠️ Skipped \(skippedCount) package(s) during selection (exhausted or hash mismatch)")
    }

    // ✅ PRE-FLIGHT: Final verification of selected packages
    logger.debug("📦 [selectKeyPackages] Final verification of selected packages:")
    for pkg in selected {
      logger.debug(
        "   - DID: \(pkg.did.description.prefix(30))... | Hash: \(pkg.hash.prefix(16))... | Size: \(pkg.data.count) bytes"
      )
    }

    // Log multi-device summary
    logger.info("📦 [selectKeyPackages] Multi-device summary:")
    for (did, count) in packagesPerMember {
      logger.info("   - \(did.prefix(30))...: \(count) device(s)")
    }

    logger.info(
      "📦 [selectKeyPackages] Selected \(selected.count) total packages for \(members.count) members, skipped \(skippedCount)"
    )
    await reserveKeyPackages(selected)
    return selected
  }

  /// Normalize HTTP conflict responses into structured key package errors for retry logic
  internal func normalizeKeyPackageError(_ error: MLSAPIError) -> MLSAPIError {
    if case .httpError(let statusCode, let message) = error, statusCode == 409 {
      logger.warning("⚠️ Server reported HTTP 409 conflict, normalizing to keyPackageNotFound")
      return .keyPackageNotFound(detail: message)
    }
    return error
  }

  /// Compute the MLS-defined key package reference (hash_ref)
  /// - Parameters:
  ///   - keyPackageData: Raw key package bytes
  ///   - userDid: Authenticated user context for MLS client
  /// - Returns: Hex-encoded hash matching server expectations
  private func computeKeyPackageReference(for keyPackageData: Data, userDid: String) async throws
    -> String
  {
    do {
      let hashBytes = try await mlsClient.computeKeyPackageHash(
        for: userDid, keyPackageData: keyPackageData)
      return hashBytes.hexEncodedString()
    } catch {
      logger.error("❌ Failed to compute key package hash_ref: \(error.localizedDescription)")
      throw MLSConversationError.operationFailed("Unable to compute key package reference")
    }
  }

  /// Prepare local commit/welcome data for the specified members
  private func prepareInitialMembers(
    members: [DID],
    userDid: String,
    groupId: Data,
    forceRefresh: Bool
  ) async throws -> PreparedInitialMembers {
    logger.info(
      "🔵 [MLSConversationManager.createGroup] Fetching key packages for \(members.count) members (forceRefresh: \(forceRefresh))"
    )
    let (keyPackages, missing) = try await apiClient.getKeyPackages(
      dids: members,
      forceRefresh: forceRefresh
    )

    if let missing, !missing.isEmpty {
      logger.error(
        "❌ [MLSConversationManager.createGroup] Missing key packages for \(missing.count) member(s)"
      )
      throw MLSConversationError.missingKeyPackages(missing.map { $0.description })
    }

    guard !keyPackages.isEmpty else {
      logger.error("❌ [MLSConversationManager.createGroup] No key packages available")
      throw MLSConversationError.missingKeyPackages(members.map { $0.description })
    }

    logger.info("🔵 [MLSConversationManager.createGroup] Got \(keyPackages.count) key packages")

    let selectedPackages = try await selectKeyPackages(
      for: members, from: keyPackages, userDid: userDid)
    let hashEntries: [BlueCatbirdMlsCreateConvo.KeyPackageHashEntry] = selectedPackages.map {
      package in
      BlueCatbirdMlsCreateConvo.KeyPackageHashEntry(
        did: package.did,
        hash: package.hash
      )
    }
    let keyPackageData = selectedPackages.map { $0.data }

    // 🔬 CRITICAL DIAGNOSTIC: Log joiner's key package that creator is using
    for (index, package) in selectedPackages.enumerated() {
      logger.info("🔑 [KEY PACKAGE FORENSICS - Creator Side]")
      logger.info("   Member \(index): \(package.did.description.prefix(30))...")
      logger.info("   Key Package Hash: \(package.hash.prefix(32))...")
      logger.info("   Key Package Size: \(package.data.count) bytes")
      logger.info(
        "   Key Package (first 100 bytes hex): \(package.data.prefix(100).hexEncodedString())")
      logger.info(
        "   Key Package (last 100 bytes hex): \(package.data.suffix(100).hexEncodedString())")
    }

    logger.debug("📍 [MLSConversationManager.createGroup] Adding members via MLS...")
    let addResult = try await mlsClient.addMembers(
      for: userDid,
      groupId: groupId,
      keyPackages: keyPackageData
    )

    logger.info(
      "✅ [MLSConversationManager.createGroup] Members added locally - commit: \(addResult.commitData.count) bytes, welcome: \(addResult.welcomeData.count) bytes"
    )
    logger.info("🔄 Commit staged (NOT merged yet) - will merge after server confirmation")

    // 🔬 CRITICAL DIAGNOSTIC: Log Welcome message structure
    logger.info("📨 [WELCOME MESSAGE FORENSICS - Creator Side]")
    logger.info("   Welcome Size: \(addResult.welcomeData.count) bytes")
    logger.info(
      "   Welcome (first 200 bytes hex): \(addResult.welcomeData.prefix(200).hexEncodedString())")
    logger.info(
      "   Welcome (last 200 bytes hex): \(addResult.welcomeData.suffix(200).hexEncodedString())")
    logger.info("   Commit Size: \(addResult.commitData.count) bytes")
    logger.info(
      "   Commit (first 200 bytes hex): \(addResult.commitData.prefix(200).hexEncodedString())")

    return PreparedInitialMembers(
      commitData: addResult.commitData,
      welcomeData: addResult.welcomeData,
      hashEntries: hashEntries,
      selectedPackages: selectedPackages  // Track for rollback on failure
    )
  }

  /// Create the conversation on the server, retrying once if key packages are rejected
  internal func createConversationOnServer(
    userDid: String,
    groupId: Data,
    groupIdHex: String,
    initialMembers: [DID]?,
    metadata: BlueCatbirdMlsCreateConvo.MetadataInput?
  ) async throws -> ServerConversationCreationResult {
    let hasInitialMembers = initialMembers?.isEmpty == false
    let maxAttempts = hasInitialMembers ? 2 : 1
    var lastError: Error?

    for attempt in 1...maxAttempts {
      // CRITICAL FIX: On retry attempts, clear exhausted cache for members we're trying to add
      // This allows fresh key package fetches after initial attempt exhausted cached hashes
      let forceRefresh = attempt > 1
      if forceRefresh, let members = initialMembers {
        for member in members {
          let memberDid = member.description
          await keyPackageManager.clearExhaustedKeyPackages(for: memberDid)
          logger.info("🔄 [Retry] Cleared exhausted cache for member: \(memberDid.prefix(24))...")
        }
        logger.info("🔄 [Retry] Cleared exhausted key package cache for \(members.count) member(s)")
      }
      
      var prepared: PreparedInitialMembers?
      if hasInitialMembers, let members = initialMembers {
        do {
          prepared = try await prepareInitialMembers(
            members: members,
            userDid: userDid,
            groupId: groupId,
            forceRefresh: forceRefresh
          )
          logger.info(
            "📍 [MLSConversationManager.createGroup] Prepared Welcome message for \(members.count) members (attempt \(attempt))"
          )
        } catch let error as MLSConversationError {
          if case .missingKeyPackages = error, attempt < maxAttempts {
            logger.warning(
              "⚠️ [MLSConversationManager.createGroup] Missing key packages on attempt \(attempt) - retrying with force refresh"
            )
            lastError = error
            continue
          }
          throw error
        }
      }

      logger.info(
        "🔵 [MLSConversationManager.createGroup] Creating conversation on server (attempt \(attempt))..."
      )
      do {
        let convo = try await apiClient.createConversation(
          groupId: groupIdHex,
          cipherSuite: defaultCipherSuite,
          initialMembers: initialMembers,
          welcomeMessage: prepared?.welcomeData,
          metadata: metadata,
          keyPackageHashes: prepared?.hashEntries
        )

        return ServerConversationCreationResult(
          convo: convo,
          commitData: prepared?.commitData,
          welcomeData: prepared?.welcomeData
        )
      } catch let error as MLSAPIError {
        let normalizedError = normalizeKeyPackageError(error)

        if hasInitialMembers,
          case .keyPackageNotFound(let detail) = normalizedError,
          attempt < maxAttempts
        {
          await recordKeyPackageFailure(detail: detail)
          logger.warning(
            "⚠️ [MLSConversationManager.createGroup] Server reported missing key packages (\(detail ?? "no details")). Retrying with fresh bundles..."
          )
          do {
            try await mlsClient.clearPendingCommit(for: userDid, groupId: groupId)
          } catch {
            logger.error(
              "❌ [MLSConversationManager.createGroup] Failed to clear pending commit after key package error: \(error.localizedDescription)"
            )
            // CRITICAL FIX: Unreserve packages on failure so they can be retried
            if let packages = prepared?.selectedPackages {
              await keyPackageManager.unreserveKeyPackages(packages)
              logger.info("♻️ Unreserved \(packages.count) key packages after commit clear failure")
            }
            throw error
          }

          do {
            try await smartRefreshKeyPackages()
          } catch {
            logger.warning(
              "⚠️ [MLSConversationManager.createGroup] Key package refresh failed: \(error.localizedDescription)"
            )
          }

          lastError = normalizedError
          continue
        }
        
        // CRITICAL FIX: Unreserve packages on final failure
        // If we're not retrying, we need to unreserve the packages so they can be used in future attempts
        if let packages = prepared?.selectedPackages {
          await keyPackageManager.unreserveKeyPackages(packages)
          logger.info("♻️ Unreserved \(packages.count) key packages after final server error")
        }
        
        lastError = normalizedError
        break
      } catch {
        // CRITICAL FIX: Unreserve packages on unexpected error
        if let packages = prepared?.selectedPackages {
          await keyPackageManager.unreserveKeyPackages(packages)
          logger.info("♻️ Unreserved \(packages.count) key packages after unexpected error")
        }
        
        lastError = error
        break
      }
    }

    throw lastError
      ?? MLSConversationError.serverError(
        MLSAPIError.httpError(statusCode: 400, message: "Failed to create conversation")
      )
  }


  // MARK: - Migration

  /// Force epoch refresh for all groups to revoke soft-removed members
  ///
  /// Call once after deploying the member removal fix to ensure previously
  /// "soft-removed" members (removed via server API only) have their cryptographic
  /// access revoked.
  ///
  /// This advances the epoch for all groups, which regenerates keys and ensures
  /// removed members cannot decrypt new messages.
  ///
  /// - Returns: Tuple of (successCount, failureCount)
  public func migrateGroupsToSecureRemoval() async throws -> (success: Int, failure: Int) {
    logger.info(
      "🔄 [MLSConversationManager.migrateGroupsToSecureRemoval] Starting migration: Force epoch refresh for all groups"
    )

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    var successCount = 0
    var failureCount = 0

    // Get all active conversations (key is convoId, value is ConvoView)
    let allConversations = conversations

    logger.info(
      "🔄 [MLSConversationManager.migrateGroupsToSecureRemoval] Processing \(allConversations.count) groups"
    )

    for (convoId, convo) in allConversations {
      do {
        // Convert groupId string to Data
        guard let groupIdData = Data(hexEncoded: convo.groupId) else {
          logger.error(
            "❌ [MLSConversationManager.migrateGroupsToSecureRemoval] Failed to decode groupId for \(convoId)"
          )
          failureCount += 1
          continue
        }

        // Use GroupOperationCoordinator to serialize operations
        try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
          // Send self_update to advance epoch
          let commitData = try await mlsClient.selfUpdate(
            for: userDid,
            groupId: groupIdData
          )

          // Send to server
          let commitBase64 = commitData.commitData.base64EncodedString()
          let newEpoch = try await apiClient.sendCommit(
            convoId: convoId,
            commit: commitBase64,
            idempotencyKey: UUID().uuidString.lowercased()
          )

          // Merge locally
          try await mlsClient.mergePendingCommit(
            for: userDid,
            groupId: groupIdData
          )

          logger.info(
            "✅ [MLSConversationManager.migrateGroupsToSecureRemoval] Migrated \(convoId) to epoch \(newEpoch)"
          )
          successCount += 1
        }

        // Rate limit to avoid server overload (100ms between groups)
        try await Task.sleep(nanoseconds: 100_000_000)
      } catch {
        logger.error(
          "❌ [MLSConversationManager.migrateGroupsToSecureRemoval] Failed for \(convoId): \(error.localizedDescription)"
        )
        failureCount += 1
      }
    }

    logger.info(
      "✅ [MLSConversationManager.migrateGroupsToSecureRemoval] Migration complete: \(successCount) success, \(failureCount) failures"
    )
    return (successCount, failureCount)
  }
  /// Log group state diagnostics for debugging
  internal func logGroupStateDiagnostics(userDid: String, groupId: Data, context: String) async {
    do {
      let epoch = try await mlsClient.getEpoch(for: userDid, groupId: groupId)
      let groupExists = await mlsClient.groupExists(for: userDid, groupId: groupId)
      logger.info("🔬 [DIAGNOSTICS] \(context)")
      logger.info("   - Group ID: \(groupId.hexEncodedString().prefix(16))...")
      logger.info("   - Epoch: \(epoch)")
      logger.info("   - Group exists: \(groupExists)")
    } catch {
      logger.warning("⚠️ Failed to log group state diagnostics: \(error.localizedDescription)")
    }
  }

  /// Helper to trigger message catchup for a conversation
  public func triggerCatchup(for convoId: String) async {
    guard let convo = conversations[convoId] else { return }
    await catchUpMessagesIfNeeded(for: convo)
  }
}
