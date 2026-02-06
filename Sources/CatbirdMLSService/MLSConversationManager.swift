import CatbirdMLSCore
import CryptoKit
import Foundation
import GRDB
import Observation
import OSLog
import Petrel
import Synchronization
// MLSConversationError moved to MLSTypes.swift


// Coordinators moved to MLSCoordinators.swift

/// MLS group state tracking
// MLSGroupState moved to MLSTypes.swift


// Structs moved to MLSTypes.swift


/// Pending operation
// Final types moved to MLSTypes.swift


// MARK: - Conversation Manager

/// Tracks conversation initialization state to prevent race conditions
public enum ConversationInitState: Sendable {
  case initializing
  case active
  case failed(String)
}

/// Main coordinator for MLS conversation management
/// Handles group initialization, member management, encryption/decryption,
/// server synchronization, key package management, and epoch updates
@Observable
public final class MLSConversationManager {
  public let logger = Logger(subsystem: "blue.catbird", category: "MLSConversationManager")

  // MARK: - Dependencies

  public let apiClient: MLSAPIClient
  public let atProtoClient: ATProtoClient
  public let mlsClient: MLSClient
  public let storage: MLSStorage
  @ObservationIgnored public var database: MLSDatabase
  public let configuration: MLSConfiguration
  
  /// Trust checker for determining if incoming conversations are from trusted senders
  /// Used to set initial requestState when syncing new conversations
  public let trustChecker: MLSTrustChecker

  /// Database manager owned by this conversation manager (one per user session)
  /// CRITICAL: Each conversation manager owns its database pool lifecycle
  public let databaseManager: MLSGRDBManager

  // MARK: - State

  /// Active conversations indexed by conversation ID
  /// Uses thread-safe ObservableMutexDictionary to prevent crashes from concurrent access
  public let conversations = ObservableMutexDictionary<String, BlueCatbirdMlsDefs.ConvoView>()

  /// MLS group states indexed by group ID
  /// Uses thread-safe ObservableMutexDictionary to prevent crashes from concurrent access
  public let groupStates = ObservableMutexDictionary<String, MLSGroupState>()

  /// Pending operations queue
  // Pending operations queue (MLSOperation type not defined - removed)

  /// Observers for state changes
  public var observers: [MLSStateObserver] = []

  /// Current user's DID
  public var userDid: String?

  /// Public accessor for current user DID (for optimistic UI)
  public var currentUserDID: String? {
    userDid
  }

  /// Sync state protected by Mutex (Swift 6 Synchronization)
  /// Using Mutex<Bool> to atomically check-and-set sync status
  public let syncState = Mutex<Bool>(false)

  /// Processing counters for MLS message handling diagnostics
  public let processingAttemptCounter = Mutex<Int64>(0)
  public let processingMutationCounter = Mutex<Int64>(0)
  public let selfDecryptFailureCounters = Mutex<[String: Int]>([:])
  
  /// FIX D: Track persistent decryption failures per message ID for nuclear rejoin
  /// When a message fails decryption 3+ times, trigger forceRejoin for the conversation
  public let persistentDecryptionFailures = Mutex<[String: Int]>([:])
  public let nuclearRejoinThreshold = 3
  
  /// Rejoin throttling to prevent concurrent/rapid rejoin storms
  public let rejoinInProgress = Mutex<Bool>(false)
  public let rejoinInProgressConversationID = Mutex<String?>(nil)
  public let rejoinAttemptTimestamps = Mutex<[String: Date]>([:])
  public let rejoinCooldownSeconds: TimeInterval = 60

  /// Public accessor for sync status (reads from mutex)
  public var isSyncing: Bool {
    get { syncState.withLock { $0 } }
    set { syncState.withLock { $0 = newValue } }
  }
  
  /// Global sync pause flag - set during account switch to reject new sync operations
  /// Using atomic bool for thread safety
  private let _isSyncPaused = Mutex<Bool>(false)

  public var isSyncPaused: Bool {
    get { _isSyncPaused.withLock { $0 } }
    set { _isSyncPaused.withLock { $0 = newValue } }
  }

  /// Initialization status
  public var isInitialized = false

  /// Background cleanup task
  public var cleanupTask: Task<Void, Never>?

  /// Background periodic sync task
  public var periodicSyncTask: Task<Void, Never>?

  /// Background orphan adoption task
  public var orphanAdoptionTask: Task<Void, Never>?

  /// Background GroupInfo refresh task to keep data fresh for External Commit
  public var groupInfoRefreshTask: Task<Void, Never>?

  /// Background task for detecting and rejoining missing conversations
  /// CRITICAL: This task MUST be tracked to ensure proper cancellation during shutdown
  /// Previously this was a fire-and-forget Task.detached which caused 40+ second hangs
  /// during account switching as External Commit operations continued running
  public var missingConversationsTask: Task<Void, Never>?

  /// Session generation ID - changes on each initialization
  /// Used to invalidate stale operations from previous account/session
  public let sessionGeneration = UUID()

  /// Active background tasks tracked for cancellation
  public var activeTasks: Set<UUID> = []
  public let activeTasksLock = NSLock()

  /// Key package monitor for smart replenishment
  public var keyPackageMonitor: MLSKeyPackageMonitor? {
    didSet {
      if let monitor = keyPackageMonitor {
        Task { await keyPackageManager.setMonitor(monitor) }
      }
    }
  }

  /// Consumption tracker for key package usage analytics
  public var consumptionTracker: MLSConsumptionTracker?

  /// Recently sent message tracking for deduplication (convoId -> (idempotencyKey -> timestamp))
  public var recentlySentMessages: [String: [String: Date]] = [:]
  private let deduplicationWindow: TimeInterval = 60  // 60 seconds
  public var deduplicationCleanupTimer: Timer?

  /// Pending sent messages for proactive own-message identification (messageID -> PendingMessage)
  /// Prevents re-processing own messages through FFI which would advance ratchet incorrectly
  public var pendingMessages: [String: PendingMessage] = [:]

  public let pendingMessagesLock = NSLock()
  public let pendingMessageTimeout: TimeInterval = 300  // 5 minutes

  /// Track conversations where the current user was explicitly removed to block unauthorized rejoins
  private var removalTombstones: Set<String> = []
  private let removalTombstoneLock = NSLock()
  private let removalTombstoneKeyPrefix = "mls.removal_tombstones."

  /// Track own commits to prevent re-processing them via SSE
  /// Maps commit hash (SHA256 of commit data) -> timestamp
  /// Commits are removed after 10 minutes to prevent unbounded growth
  public var ownCommits: [String: Date] = [:]
  public let ownCommitsLock = NSLock()
  private let ownCommitTimeout: TimeInterval = 600  // 10 minutes

  /// Track initialization state for conversations to prevent race conditions
  public var conversationStates: [String: ConversationInitState] = [:]

  /// Track group IDs currently being created to prevent reconciliation from deleting them
  /// Uses Mutex for thread-safe access from both creation and sync paths
  public let groupsBeingCreated = Mutex<Set<String>>(Set())

  /// Flag indicating the manager is preparing for shutdown/storage reset
  public var isShuttingDown = false

  // Note: isSuspending is defined in MLSConversationManager+Lifecycle.swift as a static-backed
  // computed property for 0xdead10cc prevention (extensions can't add stored instance properties)

  /// Serializes MLS message processing per conversation to avoid concurrent ratchet advances
  public let messageProcessingCoordinator = ConversationProcessingCoordinator()

  /// Current coordination generation assigned at initialization
  /// Used to detect and abort stale tasks after account switches
  public let currentCoordinationGeneration: Int

  /// Serializes MLS group operations per group ID to prevent concurrent mutations
  public let groupOperationCoordinator = GroupOperationCoordinator()

  /// Manages automatic synchronization of new devices to conversations
  public var deviceSyncManager: MLSDeviceSyncManager?
  
  /// Observes and surfaces membership changes (user/device added/removed)
  public var membershipChangeObserver: MLSMembershipChangeObserver?

  /// Manager for key packages (Phase 3 architecture)
  internal let keyPackageManager: MLSKeyPackageManager

  /// Coordinates message ordering across processes (prevents out-of-order processing)
  public let messageOrderingCoordinator = MLSMessageOrderingCoordinator()

  /// Serializes message sends per conversation to ensure strict FIFO ordering
  /// This prevents out-of-order delivery when sending rapid-fire messages
  public let sendQueueCoordinator = SendQueueCoordinator()


  // MARK: - Sync Circuit Breaker

  /// Tracks consecutive sync failures to implement circuit breaker pattern
  public var consecutiveSyncFailures: Int = 0

  /// Maximum consecutive sync failures before stopping automatic syncing
  public let maxConsecutiveSyncFailures: Int = 5

  /// Time when sync was last paused due to failures
  public var syncPausedAt: Date?

  /// How long to pause sync after circuit breaker trips (5 minutes)
  public let syncPauseDuration: TimeInterval = 300
  
  // MARK: - Foreground Sync Coordination
  
  /// Tracks when the app last entered foreground (for grace period coordination)
  public var lastForegroundTime: Date?
  
  /// Grace period during which MLS operations should wait for state reload (2 seconds)
  public let foregroundSyncGracePeriod: TimeInterval = 2.0
  
  /// Flag indicating a state reload is currently in progress
  public var isStateReloadInProgress: Bool = false
  
  /// Continuation for waiters blocking on state reload completion
  public var stateReloadWaiters: [CheckedContinuation<Void, Never>] = []

  // MARK: - Message Reorder Buffer

  /// Buffers out-of-order messages until predecessors arrive
  /// Key: conversationID, Value: (lastProcessedSeq, bufferedMessages, bufferTimeout)
  public var messageReorderBuffer: [String: MessageReorderState] = [:]
  public let messageReorderLock = NSLock()

  /// Maximum time to wait for missing predecessor messages before processing anyway
  public let messageReorderTimeout: TimeInterval = 5.0  // 5 seconds

  /// Maximum buffer size per conversation before force-flushing
  public let messageReorderMaxBufferSize: Int = 50

  // MARK: - Configuration

  /// Default cipher suite for new groups
  public let defaultCipherSuite: String = "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519"

  /// Key package refresh interval (in seconds) - reduced to 4 hours for proactive monitoring
  public let keyPackageRefreshInterval: TimeInterval = 14400  // 4 hours (was 24 hours)

  /// GroupInfo refresh interval (in seconds) - 6 hours proactive refresh
  /// Reduced from 12 hours to ensure GroupInfo doesn't expire before next refresh.
  /// GroupInfo TTL is 24 hours, so 6 hour refresh provides 4x safety margin.
  /// This is critical for External Commit - stale GroupInfo blocks new device joins.
  public let groupInfoRefreshInterval: TimeInterval = 21600  // 6 hours

  /// Maximum retry attempts for failed operations
  private let maxRetries = 3

  // MARK: - Initialization

  /// Initialize MLS Conversation Manager
  /// - Parameters:
  ///   - apiClient: MLS API client for server communication
  ///   - database: GRDB database queue for MLS storage
  ///   - userDid: Current user's DID
  ///   - storage: MLS storage layer (defaults to shared instance)
  ///   - configuration: MLS configuration (defaults to standard config)
  ///   - atProtoClient: ATProtoClient for device registration
  public init(
    apiClient: MLSAPIClient,
    database: MLSDatabase,
    userDid: String? = nil,
    storage: MLSStorage = .shared,
    configuration: MLSConfiguration = .default,
    atProtoClient: ATProtoClient,
    trustChecker: MLSTrustChecker = AlwaysTrustChecker()
  ) {
    self.apiClient = apiClient
    self.atProtoClient = atProtoClient
    self.database = database
    self.userDid = userDid
    self.mlsClient = MLSClient.shared  // Use singleton to persist groups
    self.storage = storage
    self.trustChecker = trustChecker

    // CRITICAL FIX: Create owned database manager (NOT shared singleton)
    // Each conversation manager owns its database pool lifecycle
    self.databaseManager = MLSGRDBManager()

    // Initialize Key Package Manager (Phase 3)
    self.keyPackageManager = MLSKeyPackageManager(
      client: MLSClient.shared,
      apiClient: apiClient,
      monitor: nil  // Monitor set later via property observer
    )

    self.configuration = configuration
    
    // Capture current coordination generation to detect stale task execution later
    self.currentCoordinationGeneration =
      MLSCoordinationStore.shared.getState().coordinationGeneration

    // Note: removal tombstones are handled via storage; the previous helper was removed/renamed.

    // Phase 3/4: MLSClient configuration moved to initialize() to support async actor access

    // Initialize device sync manager for multi-device support
    self.deviceSyncManager = MLSDeviceSyncManager(apiClient: apiClient, mlsClient: mlsClient)
    
    // Initialize membership change observer for transparency
    if let userDid = userDid {
      self.membershipChangeObserver = MLSMembershipChangeObserver(
        database: database,
        currentUserDID: userDid
      )
    } else {
      self.membershipChangeObserver = nil
    }

    logger.info("MLSConversationManager initialized with UniFFI client (using shared MLSClient)")
    configuration.validate()
  }

  /// Cleanup resources when conversation manager is deallocated
  /// Ensures database connections are properly closed
  deinit {
    let userDidPrefix = self.userDid?.prefix(20) ?? "unknown"
    logger.info("🧹 [deinit] MLSConversationManager deallocating for user: \(userDidPrefix)")

    // Close database connection synchronously
    // Note: deinit cannot be async, but databaseManager's deinit will close connections
    // The databaseManager actor's deinit will handle the actual database closure

    logger.info("✅ [deinit] MLSConversationManager cleanup completed")
  }

  /// Stop all network streams and pause synchronization
  /// Call this BEFORE beginning shutdown sequence
  public func stopAllStreams() {
    logger.info("🛑 [stopAllStreams] Pausing sync and stopping device streams")

    // 1. Pause sync to reject new API processing
    isSyncPaused = true

    // 2. Stop device sync polling
    Task {
      await deviceSyncManager?.stopPolling()
    }

    // 3. Note: SSE streams are managed by AppState/NetworkManager,
    // but we flag here that we are no longer accepting data.
  }

  // MARK: - Task Tracking & Generation Validation

  /// Register a background task for tracking and cancellation
  /// - Returns: Task ID to be used for unregistration
  public func registerTask() -> UUID {
    let taskId = UUID()
    activeTasksLock.withLock {
      activeTasks.insert(taskId)
    }
    return taskId
  }

  /// Unregister a background task
  public func unregisterTask(_ taskId: UUID) {
    activeTasksLock.withLock {
      activeTasks.remove(taskId)
    }
  }

  /// Cancel all active tracked tasks
  /// This does NOT cancel the stored Task properties (cleanupTask, etc.)
  /// Those are handled separately in shutdown()
  public func cancelAllTrackedTasks() {
    activeTasksLock.withLock {
      activeTasks.removeAll()
    }
  }

  /// Validate that this operation is still valid for the current session
  /// Throws if the session generation has changed (account switched)
  public func validateSessionGeneration(capturedGeneration: UUID) throws {
    guard capturedGeneration == sessionGeneration else {
      logger.warning("🛑 [Generation] Operation aborted - session generation mismatch")
      throw MLSConversationError.operationFailed("Session invalidated - account switched")
    }
  }

  // MARK: - Lifecycle Coordination

  // thrownIfShuttingDown moved to Extensions/MLSConversationManager+Lifecycle.swift

  // prepareForStorageReset moved to Extensions/MLSConversationManager+Lifecycle.swift


  // MARK: - Account Switching Lifecycle (FIX #4)

  /// Shutdown the conversation manager for account switching
  ///
  /// CRITICAL: Call this method BEFORE switching to a different user account.
  /// This ensures:
  /// 1. All background tasks are cancelled
  /// 2. The database connection is properly released
  /// 3. No stale operations from the previous user can corrupt the new user's data
  ///
  /// After calling shutdown(), you must create a NEW MLSConversationManager instance
  /// for the new user - do NOT reuse the existing instance.
  ///
  /// Note: This method has a 5-second timeout to prevent hanging during account switch.
  @MainActor
  @discardableResult
  // shutdown moved to Extensions/MLSConversationManager+Lifecycle.swift (part 1 removed)

    
  // shutdown moved to Extensions/MLSConversationManager+Lifecycle.swift (part 2 removed)

  
  // reloadStateFromDisk, ensureStateReloaded, initialize, validateGroupStates moved to Extensions/MLSConversationManager+Lifecycle.swift


  // detectAndRejoinMissingConversations, attemptRejoinWithWelcomeFallback, attemptWelcomeRejoin, fetchConversationForRejoin moved to Extensions/MLSConversationManager+Lifecycle.swift


  /// Publish current GroupInfo to the server
  /// CRITICAL: This function now throws errors - failures will propagate to callers
  /// - Throws: Error if GroupInfo export or upload fails
  // publishLatestGroupInfo moved to Extensions/MLSConversationManager+Groups.swift


  // MARK: - Group Initialization

  /// Create a new MLS group/conversation
  /// - Parameters:
  ///   - initialMembers: DIDs of initial members to add (optional)
  ///   - name: Conversation name
  ///   - description: Conversation description (optional)
  ///   - avatarUrl: Avatar URL (optional)
  /// - Returns: Created conversation view
  // createGroup moved to Extensions/MLSConversationManager+Groups.swift


  /// Join an existing group using a Welcome message
  /// - Parameter welcomeMessage: Base64-encoded Welcome message
  /// - Returns: Joined conversation view
  // joinGroup moved to Extensions/MLSConversationManager+Groups.swift


  // MARK: - Member Management

  /// Remove a member from the conversation
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDid: DID of member to remove
  // Member management (Block 1) moved to Extensions/MLSConversationManager+Members.swift


  // addMembersImpl moved to Extensions/MLSConversationManager+Members.swift


  // MARK: - Device Synchronization

  /// Add a new device to a conversation using a provided key package
  /// This is called by MLSDeviceSyncManager when processing pending device additions
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - deviceCredentialDid: The credential DID of the device being added (e.g., did:plc:user#device-uuid)
  ///   - keyPackageData: The MLS key package data for the device
  /// - Returns: The new epoch after adding the device
  public func addDeviceWithKeyPackage(
    convoId: String,
    deviceCredentialDid: String,
    keyPackageData: Data
  ) async throws -> Int {
    logger.info(
      "🔵 [MLSConversationManager.addDeviceWithKeyPackage] START - convoId: \(convoId), device: \(deviceCredentialDid)"
    )
    try throwIfShuttingDown("addDeviceWithKeyPackage")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      logger.error("❌ [MLSConversationManager.addDeviceWithKeyPackage] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupState = groupStates[convo.groupId] else {
      logger.error("❌ [MLSConversationManager.addDeviceWithKeyPackage] Group state not found")
      throw MLSConversationError.groupStateNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      logger.error("❌ [MLSConversationManager.addDeviceWithKeyPackage] Invalid groupId")
      throw MLSConversationError.invalidGroupId
    }

    // Extract user DID from device credential DID (format: did:plc:user#device-uuid)
    let userDidFromDevice: String
    if let hashIndex = deviceCredentialDid.firstIndex(of: "#") {
      userDidFromDevice = String(deviceCredentialDid[..<hashIndex])
    } else {
      userDidFromDevice = deviceCredentialDid
    }

    // Use GroupOperationCoordinator to serialize operations on this group
    return try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      // 1. Create commit locally using the provided key package
      logger.info(
        "🔵 [MLSConversationManager.addDeviceWithKeyPackage] Step 1/3: Creating staged commit...")
      let addResult = try await mlsClient.addMembers(
        for: userDid,
        groupId: groupIdData,
        keyPackages: [keyPackageData]
      )
      logger.info("✅ [MLSConversationManager.addDeviceWithKeyPackage] Staged commit created")

      // 2. Send commit and welcome to server
      logger.info(
        "🔵 [MLSConversationManager.addDeviceWithKeyPackage] Step 2/3: Sending to server...")

      // Track this commit as our own
      trackOwnCommit(addResult.commitData)

      // For device additions, we use the device credential DID (not user DID) in the server call
      // The server will validate this is a device belonging to an existing member
      let addMembersResult = try await apiClient.addMembers(
        convoId: convoId,
        didList: [],  // Empty - we're adding a device, not a new user
        commit: addResult.commitData,
        welcomeMessage: addResult.welcomeData,
        keyPackageHashes: nil  // Server already knows the key package from claim
      )

      guard addMembersResult.success else {
        logger.error("❌ [MLSConversationManager.addDeviceWithKeyPackage] Server rejected commit")
        try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
        throw MLSConversationError.operationFailed("Server rejected device addition")
      }

      let serverEpoch = addMembersResult.newEpoch

      // 3. Merge commit locally
      logger.info(
        "🔵 [MLSConversationManager.addDeviceWithKeyPackage] Step 3/3: Merging commit locally...")
      let localEpoch = try await mlsClient.mergePendingCommit(for: userDid, groupId: groupIdData)

      if localEpoch != UInt64(serverEpoch) {
        logger.warning(
          "⚠️ Epoch mismatch after device addition: local=\(localEpoch), server=\(serverEpoch)")
      }

      // Update local state
      var updatedState = groupState
      updatedState.epoch = localEpoch
      // Device additions don't add new user DIDs to members - they're devices of existing members
      groupStates[convo.groupId] = updatedState

      logger.info(
        "✅ [MLSConversationManager.addDeviceWithKeyPackage] COMPLETE - device: \(deviceCredentialDid), epoch: \(serverEpoch)"
      )

      return serverEpoch
    }
  }

  /// Get the device sync manager for SSE event wiring
  /// Call this to register for newDeviceEvent handling in your SSE subscription
  public func getDeviceSyncManager() -> MLSDeviceSyncManager? {
    return deviceSyncManager
  }

  /// Handle SSE new device event by forwarding to the device sync manager
  /// This provides the real-time path for multi-device sync instead of relying on polling
  public func handleNewDeviceSSEEvent(_ event: BlueCatbirdMlsSubscribeConvoEvents.NewDeviceEvent) async {
    guard let deviceSyncManager = deviceSyncManager else {
      logger.warning(
        "⚠️ [handleNewDeviceSSEEvent] Device sync manager not initialized - SSE new device event ignored"
      )
      return
    }
    logger.info(
      "📱 [handleNewDeviceSSEEvent] Forwarding new device event to sync manager - user: \(event.userDid), device: \(event.deviceId)"
    )
    await deviceSyncManager.handleNewDeviceEvent(event)
  }

  /// Request active members to publish fresh GroupInfo for a conversation
  /// Called when External Commit fails due to stale GroupInfo
  /// Emits SSE event to notify other members to upload fresh GroupInfo
  public func groupInfoRefresh(convoId: String) async {
    logger.info("🔄 [groupInfoRefresh] Requesting refresh for \(convoId)")

    do {
      let input = BlueCatbirdMlsGroupInfoRefresh.Input(convoId: convoId)
      let (responseCode, output) = try await apiClient.client.blue.catbird.mls.groupInfoRefresh(
        input: input)

      if responseCode == 200, let output = output {
        if output.requested {
          logger.info(
            "✅ [groupInfoRefresh] Request sent - \(output.activeMembers ?? 0) active members notified"
          )
        } else {
          logger.warning("⚠️ [groupInfoRefresh] No active members to notify for \(convoId)")
        }
      } else {
        logger.warning("⚠️ [groupInfoRefresh] Server returned \(responseCode) for \(convoId)")
      }
    } catch {
      logger.error("❌ [groupInfoRefresh] Failed: \(error.localizedDescription)")
    }
  }

  /// Request re-addition to a conversation when both Welcome and External Commit have failed
  /// Called after all rejoin attempts are exhausted to notify active members
  /// Emits SSE event to notify other members to re-add the user with fresh KeyPackages
  public func readdition(convoId: String) async {
    logger.info("🆘 [readdition] Requesting re-addition for \(convoId)")

    do {
      let (requested, activeMembers) = try await apiClient.readdition(convoId: convoId)

      if requested {
        logger.info("✅ [readdition] Request sent - \(activeMembers ?? 0) active members notified")
      } else {
        logger.warning(
          "⚠️ [readdition] No active members available to process re-addition for \(convoId)")
      }
    } catch {
      logger.error("❌ [readdition] Failed: \(error.localizedDescription)")
    }
  }

  /// Handle GroupInfo refresh request from SSE stream
  /// When another member encounters stale GroupInfo during External Commit rejoin,
  /// they request active members to publish fresh GroupInfo. This exports and uploads
  /// the current GroupInfo from our local MLS state.
  public func handleGroupInfoRefreshRequest(convoId: String) async {
    logger.info("🔄 [handleGroupInfoRefreshRequest] Processing refresh request for \(convoId)")

    guard let userDid = userDid else {
      logger.warning("⚠️ [handleGroupInfoRefreshRequest] No user DID available")
      return
    }

    // Try to resolve Group ID from memory first, then fall back to database
    var groupId: Data?

    // 1. Check in-memory cache
    if let convo = conversations[convoId], let gid = Data(hexEncoded: convo.groupId) {
      groupId = gid
    } 
    // 2. Fall back to database lookup (for cold starts/background processing)
    else {
      do {
        if let model = try await storage.fetchConversation(
          conversationID: convoId, 
          currentUserDID: userDid, 
          database: database
        ) {
          groupId = model.groupID
          logger.info("✅ [handleGroupInfoRefreshRequest] Resolved Group ID from database for \(convoId)")
        }
      } catch {
        logger.warning("⚠️ [handleGroupInfoRefreshRequest] Database lookup failed: \(error.localizedDescription)")
      }
    }

    // Get the group ID from our local conversation state
    guard let validGroupId = groupId else {
      logger.warning("⚠️ [handleGroupInfoRefreshRequest] Could not find group ID for \(convoId) (checked memory and DB)")
      return
    }

    do {
      // Export and upload fresh GroupInfo
      try await mlsClient.publishGroupInfo(for: userDid, convoId: convoId, groupId: validGroupId)
      logger.info(
        "✅ [handleGroupInfoRefreshRequest] Successfully published fresh GroupInfo for \(convoId)")
    } catch {
      logger.error(
        "❌ [handleGroupInfoRefreshRequest] Failed to publish GroupInfo for \(convoId): \(error.localizedDescription)"
      )
    }
  }

  /// Handle re-addition request from SSE stream
  /// When a member's rejoin attempts are exhausted (Welcome failed, External Commit failed),
  /// they request active members to re-add them. This method re-adds the user with fresh KeyPackages.
  ///
  /// - Parameters:
  ///   - convoId: Conversation ID where re-addition was requested
  ///   - userDidToAdd: DID of the user requesting re-addition
  public func handleReadditionRequest(convoId: String, userDidToAdd: String) async {
    logger.info(
      "🆘 [handleReadditionRequest] Processing re-addition request for user \(userDidToAdd.prefix(20))... in \(convoId)"
    )

    guard let currentUserDid = userDid else {
      logger.warning("⚠️ [handleReadditionRequest] No user DID available")
      return
    }

    // Don't process our own re-addition requests
    if userDidToAdd == currentUserDid {
      logger.debug("🔄 [handleReadditionRequest] Ignoring own re-addition request")
      return
    }

    // Verify we're an active member of the conversation (having a group state means we're joined)
    guard groupStates[convoId] != nil else {
      logger.warning("⚠️ [handleReadditionRequest] Not an active member of \(convoId)")
      return
    }

    do {
      // Re-add the user using the standard addMembers flow
      // This will fetch fresh KeyPackages and create a Welcome/Commit
      logger.info(
        "📤 [handleReadditionRequest] Re-adding user \(userDidToAdd.prefix(20))... to \(convoId)")
      try await addMembers(convoId: convoId, memberDids: [userDidToAdd])
      logger.info(
        "✅ [handleReadditionRequest] Successfully re-added user \(userDidToAdd.prefix(20))... to \(convoId)"
      )
    } catch {
      logger.error(
        "❌ [handleReadditionRequest] Failed to re-add user: \(error.localizedDescription)")
      // Don't throw - other active members may also receive the request and succeed
    }
  }

  // MARK: - Multi-Device External Commit Fallback

  /// Join a conversation via External Commit as a fallback for multi-device sync failures
  /// This is called when the device sync manager detects that a pending addition failed
  /// and the new device needs to self-join via External Commit.
  ///
  /// - Parameter convoId: The conversation ID to join
  /// - Throws: MLSConversationError if join fails
  public func joinViaExternalCommit(convoId: String) async throws {
    guard let userDid = userDid else {
      logger.error("❌ [joinViaExternalCommit] No user DID available")
      throw MLSConversationError.noAuthentication
    }

    logger.info("📱 [joinViaExternalCommit] Attempting External Commit fallback for \(convoId)")

    // Use the existing External Commit fallback infrastructure
    let groupIdHex = try await attemptExternalCommitFallback(
      convoId: convoId,
      userDid: userDid,
      reason: "Multi-device sync fallback"
    )

    // Fetch the conversation to update local state
    guard let convo = await fetchConversationForRejoin(convoId: convoId) else {
      logger.warning(
        "⚠️ [joinViaExternalCommit] Could not fetch conversation after join - local state may be stale"
      )
      return
    }

    // Update group state after join
    try await updateGroupStateAfterJoin(convo: convo, groupIdHex: groupIdHex, userDid: userDid)

    // Sync conversation to ensure everything is up to date via device sync manager
    if let deviceSyncManager = deviceSyncManager {
      await deviceSyncManager.syncConversation(convoId)
    }

    logger.info(
      "✅ [joinViaExternalCommit] Successfully joined \(convoId) via External Commit fallback")
  }

  /// Remove current user from conversation
  /// - Parameter convoId: Conversation identifier
  // leaveConversation, forceDeleteConversationLocally, forceDeleteConversation moved to Extensions/MLSConversationManager+Groups.swift


  /// Handle being removed/kicked from a conversation
  /// This is called when we detect (via SSE or sync) that we're no longer a member
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - reason: Optional reason for removal (kicked vs left vs out of sync)
  public func handleRemovedFromConversation(convoId: String, reason: MembershipChangeReason) async {
    logger.warning(
      "🚫 [handleRemovedFromConversation] Cleaning up after removal: \(convoId), reason: \(reason)")

    // Get the conversation info before cleanup
    let groupId = conversations[convoId]?.groupId ?? convoId  // Fallback to convoId if not found

    // Force delete using centralized method
    await forceDeleteConversationLocally(convoId: convoId, groupId: groupId)

    // Notify observers based on reason
    switch reason {
    case .selfLeft:
      notifyObservers(.conversationLeft(convoId))
    case .kicked(let by, let reasonText):
      notifyObservers(.kickedFromConversation(convoId: convoId, by: by, reason: reasonText))
    case .outOfSync:
      notifyObservers(.conversationNeedsRecovery(convoId: convoId, reason: .memberRemoval))
    case .connectionLost:
      // Don't notify for connection issues - may be temporary
      break
    }

    logger.info("✅ Cleanup completed for removed conversation: \(convoId)")
  }

  // MARK: - Admin Operations

  // MARK: Admin Helpers

  /// Determine if the current user is an admin of the given conversation using in-memory state with a database fallback.
  public func isCurrentUserAdmin(of convoId: String) async -> Bool {
    guard let userDid = userDid else { return false }

    if let convo = conversations[convoId],
      convo.members.contains(where: { $0.did.description == userDid && $0.isAdmin })
    {
      return true
    }

    do {
      let members = try await storage.fetchMembers(
        conversationID: convoId,
        currentUserDID: userDid,
        database: database
      )

      return members.contains { model in
        model.did == userDid && model.isActive && model.role == .admin
      }
    } catch {
      logger.error("Failed to check admin status for \(convoId): \(error.localizedDescription)")
      return false
    }
  }

  /// Determine if the current user is an admin for any conversation.
  public func isCurrentUserAdminInAnyConversation() async -> Bool {
    guard let userDid = userDid else { return false }

    if conversations.values.contains(where: { convo in
      convo.members.contains { $0.did.description == userDid && $0.isAdmin }
    }) {
      return true
    }

    do {
      let adminCount = try await database.read { db in
        try MLSMemberModel
          .filter(MLSMemberModel.Columns.currentUserDID == userDid)
          .filter(MLSMemberModel.Columns.role == MLSMemberModel.Role.admin.rawValue)
          .filter(MLSMemberModel.Columns.isActive == true)
          .fetchCount(db)
      }

      return adminCount > 0
    } catch {
      logger.error("Failed to determine admin membership: \(error.localizedDescription)")
      return false
    }
  }


  // Member management (Block 2) moved to Extensions/MLSConversationManager+Members.swift











  // MARK: - Moderation


  // Moderation methods moved to Extensions/MLSConversationManager+Members.swift


  /// Warn a member in a conversation (admin-only)
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDid: DID of member to warn
  ///   - reason: Reason for warning
  /// - Returns: Tuple of warning ID and delivery timestamp
  // warnMember moved to Extensions/MLSConversationManager+Members.swift










  // Server synchronization moved to Extensions/MLSConversationManager+Sync.swift
















}
