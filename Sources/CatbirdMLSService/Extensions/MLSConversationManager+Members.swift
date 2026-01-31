import CatbirdMLSCore
import CryptoKit
import Foundation
import GRDB
import OSLog
import Petrel
import Synchronization

public extension MLSConversationManager {
  // MARK: - Member Management

  /// Add members to an existing conversation
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDids: DIDs of members to add
  public func addMembers(convoId: String, memberDids: [String]) async throws {
    logger.info(
      "🔵 [MLSConversationManager.addMembers] START - convoId: \(convoId), members: \(memberDids.count)"
    )
    try throwIfShuttingDown("addMembers")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      logger.error("❌ [MLSConversationManager.addMembers] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupState = groupStates[convo.groupId] else {
      logger.error("❌ [MLSConversationManager.addMembers] Group state not found")
      throw MLSConversationError.groupStateNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      logger.error("❌ [MLSConversationManager.addMembers] Invalid groupId")
      throw MLSConversationError.invalidGroupId
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // 🔍 PRE-FLIGHT CHECK: Verify members aren't already in MLS group
    // ═══════════════════════════════════════════════════════════════════════════════
    do {
      let debugInfo = try await mlsClient.debugGroupMembers(for: userDid, groupId: groupIdData)
      let currentMemberDids = debugInfo.members.map {
        String(data: $0.credentialIdentity, encoding: .utf8)?.lowercased() ?? ""
      }

      // Check if any of the members we're trying to add are already in the group
      var alreadyInGroup: [String] = []
      for memberDid in memberDids {
        let normalizedDid = memberDid.lowercased()
        if currentMemberDids.contains(where: {
          $0.contains(normalizedDid) || normalizedDid.contains($0)
        }) {
          alreadyInGroup.append(memberDid)
        }
      }

      if !alreadyInGroup.isEmpty {
        logger.warning(
          "⚠️ [MLSConversationManager.addMembers] PRE-FLIGHT: \(alreadyInGroup.count) member(s) already in MLS group"
        )
        // Update groupStates to reflect actual MLS membership
        var updatedState = groupStates[convo.groupId] ?? groupState
        updatedState.members = Set(currentMemberDids)
        groupStates[convo.groupId] = updatedState
        logger.info(
          "🔄 Synced groupStates.members with MLS FFI state (\(currentMemberDids.count) members)")

        // If ALL members are already in group, throw helpful error
        if alreadyInGroup.count == memberDids.count {
          throw MLSConversationError.operationFailed(
            "All selected members are already in this conversation")
        }

        // Note: if partial membership overlap is common, we should filter existing members and add only new ones.
        throw MLSConversationError.operationFailed(
          "Some members are already in this conversation: \(alreadyInGroup.joined(separator: ", "))"
        )
      }
    } catch let error as MLSConversationError {
      throw error  // Re-throw our own errors
    } catch {
      logger.warning(
        "⚠️ [MLSConversationManager.addMembers] PRE-FLIGHT check failed, proceeding anyway: \(error.localizedDescription)"
      )
    }

    // Convert String DIDs to DID type
    let dids = try memberDids.map { try DID(didString: $0) }

    let maxAttempts = 2
    var keyPackagesWithHashes: [KeyPackageWithHash] = []
    var lastError: Error?

    for attempt in 1...maxAttempts {
      let forceRefresh = attempt > 1
      if forceRefresh {
        for did in dids {
          await keyPackageManager.clearExhaustedKeyPackages(for: did.description)
        }
        logger.info(
          "🔄 [MLSConversationManager.addMembers] Retrying key package fetch with force refresh (attempt \(attempt))"
        )
      }

      let keyPackagesResult = try await apiClient.getKeyPackages(
        dids: dids,
        forceRefresh: forceRefresh
      )

      if let missing = keyPackagesResult.missing, !missing.isEmpty {
        logger.warning("⚠️ [MLSConversationManager.addMembers] Missing key packages: \(missing)")
        let missingError = MLSConversationError.missingKeyPackages(missing.map { $0.description })
        lastError = missingError
        if attempt < maxAttempts {
          continue
        }
        throw missingError
      }

      let keyPackages = keyPackagesResult.keyPackages
      if keyPackages.isEmpty {
        logger.warning("⚠️ [MLSConversationManager.addMembers] No key packages returned")
        let emptyError = MLSConversationError.missingKeyPackages(dids.map { $0.description })
        lastError = emptyError
        if attempt < maxAttempts {
          continue
        }
        throw emptyError
      }

      do {
        keyPackagesWithHashes = try await selectKeyPackages(
          for: dids, from: keyPackages, userDid: userDid)
        break
      } catch let error as MLSConversationError {
        if case .missingKeyPackages = error, attempt < maxAttempts {
          lastError = error
          continue
        }
        throw error
      }
    }

    guard !keyPackagesWithHashes.isEmpty else {
      throw lastError ?? MLSConversationError.missingKeyPackages(dids.map { $0.description })
    }

    // Extract just the data for MLSClient
    let keyPackagesArray = keyPackagesWithHashes.map { $0.data }

    // Use GroupOperationCoordinator to serialize operations on this group
    try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      try await addMembersImpl(
        convoId: convoId,
        memberDids: memberDids,
        dids: dids,
        userDid: userDid,
        groupIdData: groupIdData,
        groupState: groupState,
        convo: convo,
        keyPackagesArray: keyPackagesArray,
        keyPackagesWithHashes: keyPackagesWithHashes
      )
    }
  }

  /// Internal implementation of addMembers (called within exclusive lock)
  internal func addMembersImpl(
    convoId: String,
    memberDids: [String],
    dids: [DID],
    userDid: String,
    groupIdData: Data,
    groupState: MLSGroupState,
    convo: BlueCatbirdMlsDefs.ConvoView,
    keyPackagesArray: [Data],
    keyPackagesWithHashes: [KeyPackageWithHash]
  ) async throws {
    do {
      // 0. Clear any stale pending commit from a previous failed operation
      do {
        try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
      } catch {
        // Ignore errors
      }

      // 1. Create commit locally (staged, not merged)
      let addResult = try await mlsClient.addMembers(
        for: userDid,
        groupId: groupIdData,
        keyPackages: keyPackagesArray
      )

      // 2. Send commit and welcome to server
      // Build key package hash entries for server lifecycle tracking
      let keyPackageHashEntries: [BlueCatbirdMlsAddMembers.KeyPackageHashEntry] =
        keyPackagesWithHashes.map { kp in
          BlueCatbirdMlsAddMembers.KeyPackageHashEntry(did: kp.did, hash: kp.hash)
        }

      // PHASE 3 FIX: Protect server send + commit merge + state update from cancellation
      let (newEpoch, mergedEpoch) = try await withTaskCancellationHandler {
        // Track this commit as our own to prevent re-processing via SSE
        trackOwnCommit(addResult.commitData)

        let addMembersResult: (success: Bool, newEpoch: Int)
        do {
          addMembersResult = try await apiClient.addMembers(
            convoId: convoId,
            didList: dids,
            commit: addResult.commitData,
            welcomeMessage: addResult.welcomeData,
            keyPackageHashes: keyPackageHashEntries
          )
        } catch let apiError as MLSAPIError {
          let normalizedError = normalizeKeyPackageError(apiError)
          switch normalizedError {
          case .keyPackageNotFound(let detail):
              await recordKeyPackageFailure(detail: detail)
            throw MLSConversationError.missingKeyPackages(memberDids)
          case .conversationNotFound:
            throw MLSConversationError.conversationNotFound
          case .notConversationMember:
            throw MLSConversationError.groupNotInitialized
          case .memberAlreadyExists:
            throw MLSConversationError.operationFailed(
              "One or more members are already part of this conversation")
          case .memberBlocked, .mutualBlockDetected:
            throw MLSConversationError.operationFailed(
              "Cannot add members due to Bluesky block relationships")
          case .tooManyMembers:
            throw MLSConversationError.operationFailed(
              "Adding these members would exceed the maximum allowed")
          default:
            throw MLSConversationError.serverError(normalizedError)
          }
        }

        guard addMembersResult.success else {
          try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
          throw MLSConversationError.operationFailed("Server rejected member addition")
        }
        let newEpoch = addMembersResult.newEpoch

        // ✅ RATCHET DESYNC FIX: Merge commit ONLY after server confirmation
        let mergedEpoch = try await mlsClient.mergePendingCommit(for: userDid, groupId: groupIdData)
        return (newEpoch, mergedEpoch)
      } onCancel: {
        logger.warning(
          "⚠️ [addMembers] Commit operation was cancelled - allowing completion to prevent epoch desync"
        )
      }

      // 3. Update local state
      var updatedState = groupStates[convo.groupId] ?? groupState
      updatedState.epoch = UInt64(newEpoch)
      updatedState.members.formUnion(memberDids)
      groupStates[convo.groupId] = updatedState

      // Publish updated GroupInfo after membership change
      try await publishLatestGroupInfo(
        userDid: userDid,
        convoId: convoId,
        groupId: groupIdData,
        context: "after addMembers"
      )

      // Record membership events in database
      do {
        for did in dids {
          let event = MLSMembershipEventModel(
            conversationID: convoId,
            currentUserDID: userDid,
            memberDID: did.description,
            eventType: .joined,
            epoch: Int64(newEpoch)
          )
          try await storage.recordMembershipEvent(event, database: database)
        }
        try await storage.updateConversationMembershipTimestamp(
          conversationID: convoId, currentUserDID: userDid, database: database)

        for did in dids {
          notifyObservers(.membershipChanged(convoId: convoId, did: did, action: .joined))
        }
      } catch {
        logger.error("Failed to record membership events: \(error.localizedDescription)")
      }

      // Also notify with legacy events
      notifyObservers(.membersAdded(convoId, dids))
      notifyObservers(.epochUpdated(convoId, Int(newEpoch)))

      // Track key package consumption
      Task {
        do {
          try await keyPackageMonitor?.trackConsumption(
            count: memberDids.count,
            operation: .addMembers,
            context: "Added \(memberDids.count) members to conversation \(convoId)"
          )
          try await smartRefreshKeyPackages()
        } catch {
          logger.warning("⚠️ Failed to track consumption or refresh: \(error.localizedDescription)")
        }
      }

      logger.info(
        "✅ [MLSConversationManager.addMembers] COMPLETE - convoId: \(convoId), epoch: \(newEpoch), members: \(updatedState.members.count)"
      )

    } catch {
      logger.error(
        "❌ [MLSConversationManager.addMembers] Error, cleaning up: \(error.localizedDescription)")

      do {
        try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
      } catch {
        logger.error(
          "❌ [MLSConversationManager.addMembers] Failed to clear pending commit: \(error.localizedDescription)"
        )
      }

      // Unreserve key packages on errors where they weren't actually consumed
      var shouldUnreserve = false

      if case .memberAlreadyInGroup = error as? MLSError {
        shouldUnreserve = true
      } else if case .serverError(let innerError) = error as? MLSConversationError,
        case .httpError(let statusCode, _) = innerError as? MLSAPIError,
        (500...599).contains(statusCode)
      {
        shouldUnreserve = true
      } else if let apiError = error as? MLSAPIError,
        case .httpError(let statusCode, _) = apiError,
        (500...599).contains(statusCode)
      {
        shouldUnreserve = true
      } else if case .operationFailed = error as? MLSError {
        shouldUnreserve = true
      }

      if shouldUnreserve {
        await keyPackageManager.unreserveKeyPackages(keyPackagesWithHashes)
      }

      if case .memberAlreadyInGroup = error as? MLSError {
        throw MLSConversationError.operationFailed(
          "Member is already in this conversation - please refresh the member list")
      }

      throw MLSConversationError.serverError(error)
    }
  }

  /// Remove a member from conversation (admin-only)
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDid: DID of member to remove
  ///   - reason: Optional reason for removal
  public func removeMember(from convoId: String, memberDid: String, reason: String? = nil) async throws {
    logger.info(
      "🔵 [MLSConversationManager.removeMember] START - convoId: \(convoId), memberDid: \(memberDid)"
    )

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      logger.error("❌ [MLSConversationManager.removeMember] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.operationFailed("Failed to decode groupId hex string")
    }

    guard let memberIdentity = memberDid.data(using: .utf8) else {
      throw MLSConversationError.operationFailed("Failed to encode member DID")
    }

    do {
      try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        do {
          try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
        } catch {
          // Ignore errors
        }

        let commitData = try await mlsClient.removeMembers(
          for: userDid, groupId: groupIdData, memberIdentities: [memberIdentity])

        let idempotencyKey = UUID().uuidString.lowercased()
        let targetDid = try DID(didString: memberDid)
        let commitBase64 = commitData.base64EncodedString()

        let (ok, epochHint) = try await apiClient.removeMember(
          convoId: convoId,
          targetDid: targetDid,
          reason: reason,
          commit: commitBase64,
          idempotencyKey: idempotencyKey
        )

        guard ok else {
          try? await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
          throw MLSConversationError.operationFailed("Server rejected member removal")
        }

        logger.info(
          "🔵 [MLSConversationManager.removeMember] Server authorized removal - epochHint: \(epochHint.map { String($0) } ?? "nil")"
        )

        try await mlsClient.mergePendingCommit(for: userDid, groupId: groupIdData, convoId: convoId)
        let newEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

        // Record membership event
        do {
          let event = MLSMembershipEventModel(
            conversationID: convoId,
            currentUserDID: userDid,
            memberDID: memberDid,
            eventType: .left,
            epoch: Int64(newEpoch)
          )
          try await storage.recordMembershipEvent(event, database: database)
          try await storage.updateConversationMembershipTimestamp(
            conversationID: convoId, currentUserDID: userDid, database: database)
          notifyObservers(.membershipChanged(convoId: convoId, did: targetDid, action: .removed))
        } catch {
          logger.error(
            "Failed to record membership event for removal: \(error.localizedDescription)")
        }

        try await syncGroupState(for: convoId)
      }
    } catch {
      logger.error("❌ [MLSConversationManager.removeMember] Failed: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  /// Promote a member to admin
  public func promoteAdmin(convoId: String, memberDid: String) async throws {
    logger.info(
      "🔵 [MLSConversationManager.promoteAdmin] START - convoId: \(convoId), member: \(memberDid)")
    try throwIfShuttingDown("promoteAdmin")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    let targetDid = try DID(didString: memberDid)

    try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      let success = try await apiClient.promoteAdmin(convoId: convoId, targetDid: targetDid)

      guard success else {
        throw MLSConversationError.operationFailed("Server failed to promote admin")
      }

      logger.info("✅ [MLSConversationManager.promoteAdmin] Success")

      try await syncGroupState(for: convoId)

      // Force refresh conversation metadata
      let (convos, _) = try await apiClient.getConversations(limit: 100)
      if let updatedConvo = convos.first(where: { $0.groupId == convo.groupId }) {
        conversations[convoId] = updatedConvo
        notifyObservers(.conversationJoined(updatedConvo))
      }
    }
  }

  /// Demote an admin to member
  public func demoteAdmin(convoId: String, memberDid: String) async throws {
    logger.info(
      "🔵 [MLSConversationManager.demoteAdmin] START - convoId: \(convoId), member: \(memberDid)")
    try throwIfShuttingDown("demoteAdmin")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    let targetDid = try DID(didString: memberDid)

    try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      let success = try await apiClient.demoteAdmin(convoId: convoId, targetDid: targetDid)

      guard success else {
        throw MLSConversationError.operationFailed("Server failed to demote admin")
      }

      logger.info("✅ [MLSConversationManager.demoteAdmin] Success")

      try await syncGroupState(for: convoId)

      // Force refresh conversation metadata
      let (convos, _) = try await apiClient.getConversations(limit: 100)
      if let updatedConvo = convos.first(where: { $0.groupId == convo.groupId }) {
        conversations[convoId] = updatedConvo
        notifyObservers(.conversationJoined(updatedConvo))
      }
    }
  }

  // MARK: - Moderation

  public func reportMember(in convoId: String, memberDid: String, reason: String, details: String? = nil)
    async throws -> String
  {
    logger.info(
      "🔵 [MLSConversationManager.reportMember] START - convoId: \(convoId), memberDid: \(memberDid), reason: \(reason)"
    )

    guard conversations[convoId] != nil else {
      logger.error("❌ [MLSConversationManager.reportMember] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    do {
      let reportId = try await apiClient.reportMember(
        convoId: convoId,
        targetDid: try DID(didString: memberDid),
        reason: reason,
        details: details
      )

      logger.info("✅ [MLSConversationManager.reportMember] SUCCESS - reportId: \(reportId)")
      return reportId
    } catch {
      logger.error("❌ [MLSConversationManager.reportMember] Failed: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  public func loadReports(for convoId: String, limit: Int = 50, cursor: String? = nil) async throws -> (
    reports: [BlueCatbirdMlsGetReports.ReportView], cursor: String?
  ) {
    logger.info("🔵 [MLSConversationManager.loadReports] START - convoId: \(convoId)")

    guard conversations[convoId] != nil else {
      logger.error("❌ [MLSConversationManager.loadReports] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    do {
      let (reports, nextCursor) = try await apiClient.getReports(
        convoId: convoId,
        limit: limit,
        cursor: cursor
      )

      logger.info("✅ [MLSConversationManager.loadReports] SUCCESS - \(reports.count) reports")
      return (reports, nextCursor)
    } catch {
      logger.error("❌ [MLSConversationManager.loadReports] Failed: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  public func resolveReport(_ reportId: String, action: String, notes: String? = nil) async throws {
    logger.info(
      "🔵 [MLSConversationManager.resolveReport] START - reportId: \(reportId), action: \(action)")

    do {
      let ok = try await apiClient.resolveReport(
        reportId: reportId,
        action: action,
        notes: notes
      )

      guard ok else {
        throw MLSConversationError.serverError(
          NSError(
            domain: "MLSConversationManager", code: -1,
            userInfo: [NSLocalizedDescriptionKey: "Server returned failure for resolveReport"]))
      }

      logger.info("✅ [MLSConversationManager.resolveReport] SUCCESS")
    } catch {
      logger.error("❌ [MLSConversationManager.resolveReport] Failed: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  /// Warn a member in a conversation (admin-only)
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDid: DID of member to warn
  ///   - reason: Reason for warning
  /// - Returns: Tuple of warning ID and delivery timestamp
  public func warnMember(in convoId: String, memberDid: String, reason: String) async throws -> (
    warningId: String, deliveredAt: Date
  ) {
    logger.info(
      "🔵 [MLSConversationManager.warnMember] START - convoId: \(convoId), memberDid: \(memberDid)")

    guard conversations[convoId] != nil else {
      logger.error("❌ [MLSConversationManager.warnMember] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    do {
      let (warningId, deliveredAt) = try await apiClient.warnMember(
        convoId: convoId,
        memberDid: try DID(didString: memberDid),
        reason: reason
      )

      logger.info("✅ [MLSConversationManager.warnMember] SUCCESS - warningId: \(warningId)")
      return (warningId, deliveredAt)

    } catch {
      logger.error("❌ [MLSConversationManager.warnMember] Failed: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }
}
