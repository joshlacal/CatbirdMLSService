import CatbirdMLSCore
import Combine
import Foundation
import GRDB
import OSLog
import Petrel

#if os(iOS)
  import UIKit
#endif

/// Modern MLS wrapper using UniFFI bindings
/// This replaces the legacy C FFI approach with type-safe Swift APIs
public actor MLSClient {
  /// Shared singleton instance - MLS context must persist across app lifetime
  /// to maintain group state in memory and keychain persistence
  public static let shared = MLSClient()

  /// Per-user MLS contexts to prevent state contamination
  /// With SQLite storage, persistence is automatic - no manual hydration needed
  private var contexts: [String: MlsContext] = [:]

  /// Per-user generation token.
  /// Bump this before account switches / storage resets so in-flight tasks fail fast.
  private var generations: [String: UInt64] = [:]

  /// Per-user API clients for server operations
  private var apiClients: [String: MLSAPIClient] = [:]

  /// Per-user device managers for multi-device support
  private var deviceManagers: [String: MLSDeviceManager] = [:]

  /// Per-user recovery managers for silent auto-recovery from desync
  private var recoveryManagers: [String: MLSRecoveryManager] = [:]

  /// Optional app-provided coordinator for storage maintenance flows.
  private var storageMaintenanceCoordinator: MLSStorageMaintenanceCoordinating?

  private let logger = Logger(
    subsystem: Bundle.main.bundleIdentifier ?? "blue.catbird", category: "MLSClient")
  private var cancellables = Set<AnyCancellable>()

  // MARK: - Initialization

  private init() {
    logger.info("🔐 MLSClient initialized with per-user context isolation")

    // Configure keychain access group for shared access between app and extensions
    // This allows NotificationServiceExtension to access MLS encryption keys
    #if os(iOS)
      #if targetEnvironment(simulator)
        // Simulator bug: Keychain access groups don't work reliably
        // Use nil to fall back to default keychain (no sharing, but prevents -34018 error)
        MLSKeychainManager.shared.accessGroup = nil
        logger.warning("⚠️ Running on simulator - keychain access group disabled (sharing won't work)")
      #else
        // Device: shared access between app and extensions (must match Keychain Sharing entitlement).
        let accessGroup = MLSKeychainManager.resolvedAccessGroup(suffix: "blue.catbird.shared")
        MLSKeychainManager.shared.accessGroup = accessGroup
        logger.debug("🔑 Configured keychain access group: \(accessGroup ?? "nil")")
      #endif
    #endif

    // Lifecycle observers are handled by AppState/AuthManager, not here
    // See setupLifecycleObservers() documentation for rationale
    logger.debug("📍 [MLSClient.init] Complete")
  }

  /// Configure the MLS API client (Phase 3/4)
  /// Must be called before using Welcome validation or bundle monitoring
  public func configure(for userDID: String, apiClient: MLSAPIClient, atProtoClient: ATProtoClient) {
    let normalizedDID = normalizeUserDID(userDID)
    self.apiClients[normalizedDID] = apiClient

    // Create managers for this specific user context
    self.deviceManagers[normalizedDID] = MLSDeviceManager(
      apiClient: atProtoClient, mlsAPIClient: apiClient, mlsClient: self)

    self.recoveryManagers[normalizedDID] = MLSRecoveryManager(
      mlsClient: self, mlsAPIClient: apiClient)

    logger.info(
      "✅ MLSClient configured for user \(normalizedDID.prefix(20))... with API client, device manager, and recovery manager"
    )
  }

  /// Invalidate cached API client for a user (called after E2E re-login)
  /// This ensures subsequent MLS operations use the fresh client with new tokens
  public func invalidateCachedClient(for userDID: String) {
    let normalizedDID = normalizeUserDID(userDID)
    apiClients.removeValue(forKey: normalizedDID)
    deviceManagers.removeValue(forKey: normalizedDID)
    recoveryManagers.removeValue(forKey: normalizedDID)
    logger.info("[E2E] Invalidated cached MLS clients for \(normalizedDID.prefix(20))...")
  }

  /// Provide an app-level storage maintenance coordinator (optional).
  public func setStorageMaintenanceCoordinator(_ coordinator: MLSStorageMaintenanceCoordinating?) {
    storageMaintenanceCoordinator = coordinator
  }

  /// Get the recovery manager for error handling
  public func recovery(for userDID: String) -> MLSRecoveryManager? {
    let normalizedDID = normalizeUserDID(userDID)
    return recoveryManagers[normalizedDID]
  }

  /// Ensure device is registered and get MLS DID
  /// Must be called before creating key packages
  public func ensureDeviceRegistered(userDid: String) async throws -> String {
    let normalizedDID = normalizeUserDID(userDid)
    guard let deviceManager = deviceManagers[normalizedDID] else {
      logger.error(
        "❌ Device manager not configured for user \(normalizedDID) - call configure() first")
      throw MLSError.configurationError
    }
    return try await deviceManager.ensureDeviceRegistered(userDid: userDid)
  }

  /// Get device info for key package uploads for a specific user
  /// - Parameter userDID: The user's DID
  /// - Returns: Device info tuple or nil if not registered
  public func getDeviceInfo(for userDID: String) async -> (
    deviceId: String, mlsDid: String, deviceUUID: String?
  )? {
    let normalizedDID = normalizeUserDID(userDID)
    return await deviceManagers[normalizedDID]?.getDeviceInfo(for: userDID)
  }

  /// Get the MLS client identity for a user on this device.
  /// Returns `did#deviceUUID` format for proper multi-device MLS support.
  ///
  /// - Parameter userDID: The user's DID
  /// - Returns: Client identity or nil if device not registered
  public func getClientIdentity(for userDID: String) async -> String? {
    let normalizedDID = normalizeUserDID(userDID)
    return await deviceManagers[normalizedDID]?.getClientIdentity(for: userDID)
  }

  /// Force re-registration of device with fresh key packages
  /// Used by recovery manager for silent recovery from desync
  public func reregisterDevice(for userDid: String) async throws -> String {
    let normalizedDID = normalizeUserDID(userDid)
    guard let deviceManager = deviceManagers[normalizedDID] else {
      logger.error(
        "❌ Device manager not configured for user \(normalizedDID) - call configure() first")
      throw MLSError.configurationError
    }
    return try await deviceManager.reregisterDevice(userDid: userDid)
  }

  /// Execute FFI operation on background thread to prevent MainActor blocking
  private func runFFI<T: Sendable>(_ operation: @Sendable @escaping () throws -> T) async throws
    -> T
  {
    try await withCheckedThrowingContinuation { continuation in
      DispatchQueue.global(qos: .userInitiated).async {
        do {
          let result = try operation()
          continuation.resume(returning: result)
        } catch {
          continuation.resume(throwing: error)
        }
      }
    }
  }

  /// Execute FFI operation with automatic recovery from poisoned context
  /// If the context is poisoned (previous operation panicked), clears it and retries once
  private func runFFIWithRecoveryLocked<T: Sendable>(
    for userDID: String,
    operation: @Sendable @escaping (MlsContext) throws -> T
  ) async throws -> T {
    var context = try getContext(for: userDID)

    for attempt in 1...2 {
      do {
        return try await runFFI {
          try operation(context)
        }
      } catch let error as MlsError {
        if isPoisonedContextError(error) && attempt == 1 {
          logger.warning(
            "⚠️ [MLSClient] Context poisoned for user \(userDID.prefix(20))..., clearing and retrying (attempt \(attempt))"
          )
          clearPoisonedContext(for: userDID)
          context = try getContext(for: userDID)
          continue
        }
        throw error
      }
    }

    throw MLSError.operationFailed
  }

  /// Execute FFI operation with automatic recovery from poisoned context,
  /// serialized per-user and coordinated cross-process to avoid ratchet/db desync.
  private func runFFIWithRecovery<T: Sendable>(
    for userDID: String,
    operation: @Sendable @escaping (MlsContext) throws -> T
  ) async throws -> T {
    let normalizedDID = normalizeUserDID(userDID)
    let generation = currentGeneration(for: normalizedDID)

    return try await withMLSUserPermit(for: normalizedDID) {
      try assertGeneration(generation, for: normalizedDID)

      // No advisory lock needed - SQLite WAL handles concurrent access
      // Cross-process coordination uses Darwin notifications (MLSCrossProcess)

      try assertGeneration(generation, for: normalizedDID)

      // Direct FFI call
      let result = try await self.runFFIWithRecoveryLocked(for: normalizedDID, operation: operation)

      try assertGeneration(generation, for: normalizedDID)
      return result
    }
  }

  /// Normalize user DID to ensure consistent context lookup
  /// Prevents multiple contexts for the same user due to whitespace/encoding differences
  private func normalizeUserDID(_ userDID: String) -> String {
    return userDID.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  private func currentGeneration(for normalizedDID: String) -> UInt64 {
    generations[normalizedDID] ?? 0
  }

  public func bumpGeneration(for userDID: String) {
    let normalizedDID = normalizeUserDID(userDID)
    let next = (generations[normalizedDID] ?? 0) &+ 1
    generations[normalizedDID] = next
    logger.debug("🔁 [MLSClient] Bumped generation for \(normalizedDID.prefix(20))... → \(next)")
  }

  private func assertGeneration(_ captured: UInt64, for normalizedDID: String) throws {
    if currentGeneration(for: normalizedDID) != captured {
      throw CancellationError()
    }
  }

  /// Check if an MlsError indicates a poisoned/unrecoverable context
  /// This happens when a previous FFI operation panicked while holding the Mutex lock
  private func isPoisonedContextError(_ error: MlsError) -> Bool {
    if case .ContextNotInitialized = error {
      return true
    }
    return false
  }

  /// Clear a poisoned context from the cache to allow recovery on next attempt
  /// Call this when FFI operations fail with ContextNotInitialized
  private func clearPoisonedContext(for userDID: String) {
    let normalizedDID = normalizeUserDID(userDID)
    if contexts.removeValue(forKey: normalizedDID) != nil {
      logger.warning(
        "🔄 [MLSClient] Cleared poisoned context for user: \(normalizedDID.prefix(20))... (will recreate on next operation)"
      )
    }
  }

  /// Get or create a context for a specific user.
  private func getContext(for userDID: String) throws -> MlsContext {
    let normalizedDID = normalizeUserDID(userDID)

    if let existingContext = contexts[normalizedDID] {
      return existingContext
    }

    // CRITICAL: Log full DID when creating new context for debugging
    logger.info("🆕 Created new MlsContext for user: \(normalizedDID.prefix(20))...")
    logger.debug("[MLSClient] Full normalized DID: \(normalizedDID)")
    logger.debug(
      "[MLSClient] Existing context keys in cache: \(self.contexts.keys.map { $0.prefix(20) })")

    let newContext = try createContext(for: normalizedDID)
    contexts[normalizedDID] = newContext
    return newContext
  }

  /// Reload MLS context from storage for non-destructive recovery
  /// This clears the in-memory context and recreates it, forcing a reload from SQLite
  /// Returns the number of bundles found after reload
  private func reloadContextFromStorage(for userDID: String) async throws -> UInt64 {
    let normalizedDID = normalizeUserDID(userDID)
    logger.info(
      "🔄 [Recovery] Attempting non-destructive context reload for user: \(normalizedDID.prefix(20))..."
    )

    // Remove existing context from cache
    contexts.removeValue(forKey: normalizedDID)
    logger.debug("   ♻️ Cleared in-memory context from cache")

    // Create fresh context - this will load from SQLite
    let newContext = try createContext(for: normalizedDID)
    contexts[normalizedDID] = newContext
    logger.debug("   ✅ Created fresh context from SQLite storage")

    // Check if bundles were recovered
    let bundleCount = try newContext.getKeyPackageBundleCount()
    logger.info("   📊 Bundle count after reload: \(bundleCount)")

    if bundleCount > 0 {
      logger.info("✅ [Recovery] Non-destructive recovery successful! Found \(bundleCount) bundles")
    } else {
      logger.warning(
        "⚠️ [Recovery] Non-destructive recovery found 0 bundles - may need full re-registration")
    }

    return bundleCount
  }

  /// Create a new MLS context with per-DID SQLite storage
  /// Storage path: {appSupport}/mls-state/{did_hash}.db
  private func createContext(for userDID: String) throws -> MlsContext {
    // Create storage directory if needed
    let appSupport: URL
    if let sharedContainer = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared")
    {
      appSupport = sharedContainer
    } else {
      appSupport =
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
    }
    let mlsStateDir = appSupport.appendingPathComponent("mls-state", isDirectory: true)

    do {
      try FileManager.default.createDirectory(at: mlsStateDir, withIntermediateDirectories: true)
    } catch {
      logger.error("❌ Failed to create MLS state directory: \(error.localizedDescription)")
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL: 0xdead10cc Migration - Check if database needs recreation
    // ═══════════════════════════════════════════════════════════════════════════
    // Databases created before cipher_plaintext_header_size=32 was added have
    // encrypted headers, causing iOS to fail to identify them as SQLite WAL
    // databases. This prevents automatic checkpointing during suspension,
    // leading to 0xdead10cc termination.
    //
    // This migration deletes old databases so they can be recreated with the
    // plaintext header. MLS conversation history will be lost, but this is
    // necessary to prevent 0xdead10cc crashes.
    // ═══════════════════════════════════════════════════════════════════════════
    let migrationPerformed = MLSPlaintextHeaderMigration.ensurePlaintextHeaderMigration(
      for: userDID,
      databaseType: .rustFFI
    )
    if migrationPerformed {
      logger.warning("🔧 [0xdead10cc] Rust FFI database was recreated for plaintext header migration")
    }

    // Hash the DID to create a valid filename
    let didHash =
      userDID.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(64) ?? "default"

    let storagePath = mlsStateDir.appendingPathComponent("\(didHash).db").path
    logger.info("📁 Using SQLite storage at: \(storagePath)")

    // Get encryption key from Keychain
    let encryptionKey: String
    do {
      let keyData = try MLSKeychainManager.shared.getOrCreateEncryptionKey(forUserDID: userDID)
      encryptionKey = keyData.hexEncodedString()
    } catch {
      logger.error("❌ Failed to get encryption key from Keychain: \(error.localizedDescription)")
      // Fallback to a derived key from DID if Keychain fails (better than crashing, but logs error)
      encryptionKey = String(didHash)
    }

    // Create context with per-DID SQLite storage
    // Retry logic: Attempt up to 3 times with exponential backoff
    var newContext: MlsContext?
    var lastError: Error?

    // Create keychain adapter for hybrid storage
    let keychainAdapter = MLSKeychainAdapter()

    for attempt in 1...3 {
      do {
        newContext = try MlsContext(
          storagePath: storagePath, encryptionKey: encryptionKey, keychain: keychainAdapter)
        logger.info(
          "✅ Created MlsContext with Encrypted SQLite storage (automatic persistence enabled)")
        break
      } catch {
        lastError = error
        let errorMessage = error.localizedDescription.lowercased()

        // CRITICAL FIX: Detect SQLCipher encryption key mismatch errors
        // SQLCipher returns misleading "out of memory" when the encryption key is wrong
        // BUT we need to distinguish between true key mismatch and account switching race conditions
        let isHMACFailure = errorMessage.contains("hmac check failed") ||
          errorMessage.contains("hmac verification") ||
          (errorMessage.contains("hmac") && errorMessage.contains("pgno"))

        if isHMACFailure {
          // HMAC failure during account switching = wrong key due to race condition
          // Do NOT delete the database - it's valid, just accessed with wrong key
          logger.error("🔐 [MLSClient] HMAC CHECK FAILED - possible account switching race condition!")
          logger.error("   Database is valid but accessed with wrong encryption key")
          logger.error("   This typically happens when old account's context is accessed during switch")
          logger.error("   ⚠️  NOT deleting database - will retry with correct key")

          // For HMAC failures, don't delete - just fail and let the account switch complete
          // The next attempt with correct user context will succeed
          continue
        }

        let isKeyMismatch =
          errorMessage.contains("encryption key mismatch")
          || errorMessage.contains("cannot be decrypted")
          || (errorMessage.contains("out of memory") && !isHMACFailure)

        if isKeyMismatch {
          logger.error("🔑 [MLSClient] DATABASE ENCRYPTION KEY MISMATCH DETECTED!")
          logger.error("   Database exists but cannot be decrypted with current Keychain key")
          logger.error("   This typically happens after:")
          logger.error("   1. Device restore that didn't include Keychain")
          logger.error("   2. App reinstall without Keychain backup")
          logger.error("   3. Keychain item was deleted/corrupted")
          logger.error("   ⚠️  Deleting database and re-registering device...")
        } else {
          logger.error(
            "❌ Attempt \(attempt)/3 failed to create MlsContext: \(error.localizedDescription)")
        }

        if attempt < 3 {
          // Fail-closed: never delete or rewrite storage automatically.
          // Brief sleep for exponential backoff (100ms, 200ms).
          Thread.sleep(forTimeInterval: TimeInterval(attempt) * 0.1)
        }
      }
    }

    guard let context = newContext else {
      throw lastError ?? MLSError.operationFailed
    }

    // Set up epoch secret storage for forward secrecy with message history
    let epochStorage = MLSEpochSecretStorageBridge(userDID: userDID, databaseManager: .shared)
    do {
      try context.setEpochSecretStorage(storage: epochStorage)
      logger.info("✅ Configured epoch secret storage for historical message decryption")
    } catch {
      logger.error("❌ Failed to configure epoch secret storage: \(error.localizedDescription)")
      // Non-fatal - context can still function without epoch storage
    }

    return context
  }

  // MARK: - Device Status

  /// Check if a device is currently registered for the given user
  /// - Parameter userDID: The user's DID
  /// - Returns: True if device info exists locally
  public nonisolated func isDeviceRegistered(for userDID: String) -> Bool {
    // This is a convenient non-async check against shared state if possible,
    // but MLSDeviceManager encapsulates this. We'll need to check via device manager access.
    // Since we can't easily access actor state synchronously/non-isolated,
    // we'll rely on the caller to await the actor call if needed, OR
    // we can check our own map if we trust it's up to date (it is).
    // Note: This access is racy if we don't await, but `deviceManagers` is an actor property.
    // For a `nonisolated` implementation, we'd need a separate thread-safe store.
    // Instead, we'll make this async.
    return false // Placeholder - see async version
  }

  /// Check if a device is currently registered for the given user (async)
  public func isDeviceRegisteredAsync(for userDID: String) -> Bool {
    let normalizedDID = normalizeUserDID(userDID)
    // We check if we have a device manager and if it has device info
    guard let manager = deviceManagers[normalizedDID] else { return false }
    // We can't synchronously peek into the manager actor, so we return true if manager exists
    // and rely on the manager's methods for details.
    // Actually, MLSClient *initializes* the manager, but the manager manages the persistence.
    // Let's rely on checking `deviceInfoByUser` in UserDefaults which is the source of truth,
    // but that's inside MLSDeviceManager.
    //
    // Simplified approach: If we have a device manager confgured, we assume the POTENTIAL for registration.
    // But for the specific "0 bundle" check, we need to know if we *think* we are registered.
    //
    // Let's add a helper to MLSDeviceManager first, but for now we'll route via MLSClient.
    return deviceManagers[normalizedDID] != nil
  }

  // MARK: - Key Package Management

  /// Create a batch of key packages in a single transaction
  /// This prevents lock contention and race conditions during registration
  public func batchCreateKeyPackages(for userDID: String, identity: String, count: Int) async throws -> [Data] {
    logger.info("🔐 [MLSClient] Creating batch of \(count) key packages for \(identity.prefix(20))...")
    
    // Validate count
    guard count > 0 else { return [] }
    let safeCount = min(count, 100) // Cap at 100 to prevent timeouts
    
    let identityBytes = Data(identity.utf8)
    
    // Execute all creations under ONE lock acquisition
    return try await runFFIWithRecovery(for: userDID) { ctx in
      var packages: [Data] = []
      for i in 0..<safeCount {
        // Create package
        let package = try ctx.createKeyPackage(identityBytes: identityBytes)
        packages.append(package.keyPackageData)
      }
      
      // Force a database sync after the batch (checkpoint WAL)
      // This ensures all packages are durably persisted
      try ctx.syncDatabase()
      
      return packages
    }
  }

  // MARK: - Group Management

  /// Create a new MLS group using client identity (did#deviceUUID)
  /// Each device is a unique MLS leaf node for proper multi-device support.
  public func createGroup(for userDID: String, configuration: MLSGroupConfiguration = .default)
    async throws -> Data
  {
    logger.info("📍 [MLSClient.createGroup] START - user: \(userDID.prefix(20))")

    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.createGroup] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }

    logger.debug(
      "[MLSClient.createGroup] Client identity: '\(clientIdentity)' (length: \(clientIdentity.count))"
    )

    // Log bundle count BEFORE group creation
    let context = try getContext(for: userDID)
    if let bundleCount = try? context.getKeyPackageBundleCount() {
      logger.debug("[MLSClient.createGroup] Bundle count BEFORE group creation: \(bundleCount)")
      if bundleCount == 0 {
        logger.error(
          "🚨 [MLSClient.createGroup] CRITICAL: Context has 0 bundles before group creation!")
        logger.error(
          "   This indicates bundles were lost between key package creation and group creation")
      }
    }

    // Use client identity (did#deviceUUID) as MLS credential identity
    // This allows each device to be a unique leaf node in the MLS tree
    let identityBytes = Data(clientIdentity.utf8)

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.createGroup(identityBytes: identityBytes, config: configuration)
      }
      logger.info(
        "✅ [MLSClient.createGroup] Group created - ID: \(result.groupId.hexEncodedString().prefix(16))"
      )

      // 🔒 CRITICAL FIX: Force database sync after group creation
      // This ensures the new group state is durably persisted before any subsequent operations.
      // Without this, if context gets recreated (via clearPoisonedContext) before WAL checkpoint,
      // the group would be lost and groupExists() would return false, triggering a spurious
      // External Commit rejoin for the creator - which breaks the invitee's epoch.
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.syncDatabase()
        }
        logger.info("✅ [MLSClient.createGroup] Database synced after group creation")
      } catch {
        logger.error("⚠️ [MLSClient.createGroup] Database sync failed: \(error.localizedDescription)")
        // Continue anyway - the group was created, sync failure is not fatal but may cause issues
      }

      return result.groupId
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.createGroup] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Join an existing group using a welcome message (low-level with explicit identity)
  /// Use the convenience method without identity parameter for automatic bare DID usage
  public func joinGroup(
    for userDID: String, welcome: Data, identity: String,
    configuration: MLSGroupConfiguration = .default
  ) async throws -> Data {
    logger.info(
      "📍 [MLSClient.joinGroup] START - user: \(userDID.prefix(20)), identity: \(identity.prefix(30)), welcome size: \(welcome.count) bytes"
    )
    logger.debug("[MLSClient.joinGroup] Full userDID: '\(userDID)' (length: \(userDID.count))")
    logger.debug("[MLSClient.joinGroup] Full identity: '\(identity)' (length: \(identity.count))")

    // Phase 3 validation now occurs on the sender before the Welcome is uploaded.
    // Recipients proceed directly to processing since the server has already approved the Welcome.
    let identityBytes = Data(identity.utf8)

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.processWelcome(
          welcomeBytes: welcome, identityBytes: identityBytes, config: configuration)
      }
      logger.info(
        "✅ [MLSClient.joinGroup] Joined group - ID: \(result.groupId.hexEncodedString().prefix(16))"
      )

      // 🔒 FIX #2: Force database sync after Welcome processing
      // This ensures the new group state is durably persisted before any messages are sent/received
      // Without this, app restart could cause SecretReuseError from incomplete WAL checkpoint
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.syncDatabase()
        }
        logger.info("✅ [MLSClient.joinGroup] Database synced after Welcome processing")
      } catch {
        logger.error("⚠️ [MLSClient.joinGroup] Database sync failed: \(error.localizedDescription)")
        // Continue anyway - the group was joined, sync failure is not fatal
      }

      return result.groupId
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.joinGroup] FAILED: \(error.localizedDescription)")

      // 🔍 DIAGNOSTIC: If NoMatchingKeyPackage, log local hashes for comparison
      let errorStr = String(describing: error)
      if errorStr.contains("NoMatchingKeyPackage") || errorStr.contains("no matching key package") {
        logger.error(
          "🔍 [MLSClient.joinGroup] NoMatchingKeyPackage - Listing local manifest hashes...")

        do {
          let context = try getContext(for: userDID)
          let localHashes = try context.debugListKeyPackageHashes()
          logger.error("🔍 Local manifest contains \(localHashes.count) key package hashes:")
          for (i, hash) in localHashes.prefix(10).enumerated() {
            logger.error("   [\(i)] \(hash)")
          }
          if localHashes.count > 10 {
            logger.error("   ... and \(localHashes.count - 10) more")
          }
          logger.error("🔍 Compare with the hash used in the Welcome (logged on creator side)")
        } catch {
          logger.error("🔍 Failed to list local hashes: \(error)")
        }
        
        // ⭐ CRITICAL FIX: Re-throw the original MlsError.NoMatchingKeyPackage
        // This allows initializeGroupFromWelcome to catch it and trigger External Commit fallback
        // Previously we wrapped this as MLSError.operationFailed, which lost the error type
        // and prevented the External Commit recovery path from executing
        throw error
      }

      throw MLSError.operationFailed
    }
  }

  /// Join an existing group using a welcome message with client identity (did#deviceUUID)
  /// Each device is a unique MLS leaf node for proper multi-device support.
  public func joinGroup(
    for userDID: String, welcome: Data, configuration: MLSGroupConfiguration = .default
  ) async throws -> Data {
    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.joinGroup] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    return try await joinGroup(
      for: userDID, welcome: welcome, identity: clientIdentity, configuration: configuration)
  }

  /// Join a group via External Commit using GroupInfo
  /// This allows joining without a Welcome message from an existing member
  /// Includes retry logic for transient deserialization errors (EndOfStream, truncated data)
  public func joinByExternalCommit(for userDID: String, convoId: String) async throws -> Data {
    logger.info("📍 [MLSClient.joinByExternalCommit] START - user: \(userDID), convoId: \(convoId)")

    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      throw MLSError.configurationError
    }

    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.joinByExternalCommit] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }

    let maxRetries = 3
    var lastError: Error?

    for attempt in 1...maxRetries {
      logger.info("🔄 [MLSClient.joinByExternalCommit] Attempt \(attempt)/\(maxRetries)")

      do {
        // 1. Fetch FRESH GroupInfo with metadata for each attempt
        let (groupInfo, epoch, expiresAt) = try await apiClient.getGroupInfo(convoId: convoId)

        // 2. Validate GroupInfo freshness
        if let expiresAt = expiresAt {
          if expiresAt < Date() {
            logger.error(
              "❌ [MLSClient.joinByExternalCommit] GroupInfo EXPIRED - expires: \(expiresAt), now: \(Date())"
            )
            logger.error("   GroupInfo epoch: \(epoch), size: \(groupInfo.count) bytes")
            logger.error("   External Commit cannot proceed with stale GroupInfo")

            // CRITICAL FIX: Request GroupInfo refresh from active members and wait before retrying
            // This allows recovery when GroupInfo TTL has expired
            logger.info(
              "🔄 [MLSClient.joinByExternalCommit] Requesting GroupInfo refresh from active members..."
            )
            var refreshRequested = false
            do {
              let (requested, activeMembers) = try await apiClient.groupInfoRefresh(
                convoId: convoId)
              refreshRequested = requested
              if requested {
                logger.info(
                  "✅ [MLSClient.joinByExternalCommit] GroupInfo refresh requested - \(activeMembers ?? 0) active members notified"
                )
              } else {
                logger.warning(
                  "⚠️ [MLSClient.joinByExternalCommit] No active members to refresh GroupInfo")
              }
            } catch {
              logger.warning(
                "⚠️ [MLSClient.joinByExternalCommit] Failed to request GroupInfo refresh: \(error.localizedDescription)"
              )
            }

            // If we have more retries and refresh was requested, wait and retry
            if attempt < maxRetries && refreshRequested {
              // Wait for active members to publish fresh GroupInfo
              // Use exponential backoff with jitter: base (2s/4s) + random jitter
              let baseWaitSeconds = 2 * attempt
              let jitterMs = UInt64.random(in: 0...1000)  // Up to 1s jitter
              logger.info(
                "🔄 [MLSClient.joinByExternalCommit] Waiting ~\(baseWaitSeconds)s for fresh GroupInfo before retry..."
              )
              try await Task.sleep(for: .seconds(baseWaitSeconds))
              try await Task.sleep(for: .milliseconds(jitterMs))
              continue  // Retry with fresh GroupInfo
            }

            // No more retries or refresh not possible - throw error
            throw MLSError.staleGroupInfo(
              convoId: convoId,
              message:
                "GroupInfo expired at \(expiresAt) (epoch \(epoch)) - refresh requested from active members"
            )
          } else {
            let remaining = expiresAt.timeIntervalSince(Date())
            logger.info(
              "✅ [MLSClient.joinByExternalCommit] GroupInfo valid - expires in \(Int(remaining))s")
          }
        } else {
          logger.warning(
            "⚠️ [MLSClient.joinByExternalCommit] No expiry on GroupInfo - proceeding cautiously")
        }

        // 3. Validate GroupInfo size (minimum 100 bytes for valid MLS GroupInfo)
        if groupInfo.count < 100 {
          logger.error(
            "❌ [MLSClient.joinByExternalCommit] GroupInfo suspiciously small: \(groupInfo.count) bytes"
          )
          logger.error("   Expected minimum ~100 bytes for valid MLS GroupInfo structure")
          logger.error("   First 32 bytes (hex): \(groupInfo.prefix(32).hexEncodedString())")
          throw MLSError.invalidGroupInfo(
            convoId: convoId,
            message: "GroupInfo too small: \(groupInfo.count) bytes (minimum 100 expected)"
          )
        }

        // 4. Check for base64 encoding issues (GroupInfo should be binary, not ASCII-only)
        let isAsciiOnly = groupInfo.allSatisfy { byte in
          (byte >= 0x20 && byte <= 0x7E) || byte == 0x0A || byte == 0x0D  // printable ASCII + newlines
        }
        if isAsciiOnly && groupInfo.count > 50 {
          logger.error(
            "❌ [MLSClient.joinByExternalCommit] GroupInfo appears to be text/base64 encoded!")
          logger.error("   Raw bytes appear to be ASCII text, not binary MLS data")
          logger.error("   This suggests base64 decoding was skipped somewhere")
          logger.error(
            "   First 100 chars: \(String(data: groupInfo.prefix(100), encoding: .utf8) ?? "n/a")")
          throw MLSError.invalidGroupInfo(
            convoId: convoId,
            message: "GroupInfo appears base64-encoded - decoding may have been skipped"
          )
        }

        logger.info(
          "📊 [MLSClient.joinByExternalCommit] GroupInfo validated: \(groupInfo.count) bytes, epoch \(epoch)"
        )

        // 5. Create External Commit using client identity (did#deviceUUID)
        let identityBytes = Data(clientIdentity.utf8)
        let result = try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.createExternalCommit(
            groupInfoBytes: groupInfo, identityBytes: identityBytes)
        }

        // 6. Send Commit to Server
        do {
          let _ = try await apiClient.processExternalCommit(
            convoId: convoId,
            externalCommit: result.commitData,
            groupInfo: nil  // We don't need to update GroupInfo here, just joining
          )
        } catch let apiError as MLSAPIError {
          if case .httpError(let statusCode, _) = apiError, statusCode == 403 {
            logger.warning(
              "⚠️ [MLSClient.joinByExternalCommit] External Commit rejected (HTTP 403) - requesting GroupInfo refresh"
            )
            do {
              let (requested, activeMembers) = try await apiClient.groupInfoRefresh(convoId: convoId)
              if requested {
                logger.info(
                  "✅ [MLSClient.joinByExternalCommit] GroupInfo refresh requested - \(activeMembers ?? 0) active members notified"
                )
              } else {
                logger.warning(
                  "⚠️ [MLSClient.joinByExternalCommit] No active members to refresh GroupInfo")
              }
            } catch {
              logger.warning(
                "⚠️ [MLSClient.joinByExternalCommit] GroupInfo refresh request failed: \(error.localizedDescription)"
              )
            }

            if attempt < maxRetries {
              let waitSeconds = 2 * attempt
              logger.info(
                "🔄 [MLSClient.joinByExternalCommit] Waiting ~\(waitSeconds)s before retry after 403..."
              )
              try await Task.sleep(for: .seconds(waitSeconds))
              continue
            }
          }
          throw apiError
        }

        logger.info(
          "✅ [MLSClient.joinByExternalCommit] Success - Joined group \(convoId) on attempt \(attempt)"
        )

        // Warn about epoch advancement - External Commit always advances the epoch
        // This means messages from previous epochs may be undecryptable due to forward secrecy
        do {
          let groupIdData = Data(result.groupId)
          let newEpoch = try await getEpoch(for: userDID, groupId: groupIdData)
          if newEpoch > 1 {
            logger.warning("⚠️ [EPOCH WARNING] External Commit joined at epoch \(newEpoch)")
            logger.warning(
              "   Messages from epochs 1-\(newEpoch - 1) may be undecryptable due to forward secrecy"
            )
          }
        } catch {
          // Non-fatal - just skip the warning if we can't get epoch
          logger.debug("Could not fetch epoch for warning: \(error.localizedDescription)")
        }
        
        // PHASE 3.1: Publish fresh GroupInfo after successful External Commit
        // This ensures other devices can External Commit to the new state
        // Critical for device-sync: we advanced the epoch, so we must publish the new state
        do {
          let groupIdData = Data(result.groupId)
          try await publishGroupInfo(for: userDID, convoId: convoId, groupId: groupIdData)
          logger.info("✅ [MLSClient.joinByExternalCommit] GroupInfo published after External Commit")
        } catch {
          // Non-fatal: GroupInfo upload failure shouldn't block the join
          // Another member may publish it, or we can retry later
          logger.warning("⚠️ [MLSClient.joinByExternalCommit] Failed to publish GroupInfo: \(error.localizedDescription)")
        }

        return result.groupId

      } catch let error as MlsError {
        lastError = error
        let errorMessage = error.localizedDescription.lowercased()

        // Check if this is a retriable deserialization error
        let isDeserializationError =
          errorMessage.contains("endofstream") || errorMessage.contains("deseriali")
          || errorMessage.contains("truncat") || errorMessage.contains("invalid groupinfo")
          || errorMessage.contains("malformed")

        if isDeserializationError && attempt < maxRetries {
          // Exponential backoff with jitter: base * 2^(attempt-1) + random jitter
          // Jitter helps prevent thundering herd when multiple devices retry simultaneously
          let baseDelayMs = UInt64(100 * (1 << (attempt - 1)))
          let jitterMs = UInt64.random(in: 0...(baseDelayMs / 2))
          let totalDelayMs = baseDelayMs + jitterMs
          logger.warning(
            "⚠️ [MLSClient.joinByExternalCommit] Deserialization error on attempt \(attempt): \(error.localizedDescription)"
          )
          logger.info("   🔄 Retrying in \(totalDelayMs)ms with fresh GroupInfo...")
          try await Task.sleep(for: .milliseconds(totalDelayMs))
          continue
        }

        // Non-retriable error or exhausted retries
        logger.error(
          "❌ [MLSClient.joinByExternalCommit] FAILED after \(attempt) attempt(s): \(error.localizedDescription)"
        )

        // 🔄 RECOVERY: Check if this error warrants device-level recovery
        // CRITICAL FIX: Mark as remote data error since GroupInfo comes from server
        // This prevents destructive local database wipe when server data is corrupted
        if let recoveryMgr = self.recoveryManagers[normalizedDID] {
          let errorMessage = error.localizedDescription.lowercased()
          let isServerDataCorruption =
            errorMessage.contains("invalidvectorlength") || errorMessage.contains("endofstream")
            || errorMessage.contains("malformed") || errorMessage.contains("truncat")

          if isServerDataCorruption {
            // Mark conversation as having corrupted server data - don't attempt recovery
            await recoveryMgr.markConversationServerCorrupted(
              convoId: convoId,
              errorMessage: "GroupInfo deserialization failed: \(error.localizedDescription)"
            )
            logger.error(
              "🚫 [MLSClient.joinByExternalCommit] Server data corrupted - NOT triggering local recovery"
            )
            logger.error("   GroupInfo for conversation \(convoId.prefix(16)) is malformed")
            logger.error("   Server team must investigate and republish valid GroupInfo")
          } else {
            // Only attempt recovery for LOCAL errors (e.g., key package issues)
            let recovered = await recoveryMgr.attemptRecoveryIfNeeded(
              for: error,
              userDid: userDID,
              convoIds: [convoId],
              isRemoteDataError: true  // GroupInfo is remote data
            )
            if recovered {
              logger.info(
                "🔄 [MLSClient.joinByExternalCommit] Recovery initiated - caller should retry")
            }
          }
        }

        throw MLSError.operationFailed

      } catch {
        // Non-MlsError - don't retry
        lastError = error
        logger.error(
          "❌ [MLSClient.joinByExternalCommit] Non-MLS error: \(error.localizedDescription)")
        throw error
      }
    }

    // Should never reach here, but handle gracefully
    logger.error("❌ [MLSClient.joinByExternalCommit] Exhausted all \(maxRetries) retries")
    if let error = lastError {
      throw error
    }
    throw MLSError.operationFailed
  }

  /// Manually export epoch secret for a group
  /// Call this after creating the conversation record to ensure epoch secrets persist correctly
  public func exportEpochSecret(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.exportEpochSecret] Exporting epoch secret for group: \(groupId.hexEncodedString().prefix(16))"
    )
    try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.exportEpochSecret(groupId: groupId)
    }
    logger.info("✅ [MLSClient.exportEpochSecret] Successfully exported epoch secret")
  }

  /// Minimum valid GroupInfo size in bytes
  private static let minGroupInfoSize = 100

  /// Publish GroupInfo to the server to allow external joins
  /// Should be called after any operation that advances the epoch (add, remove, update, commit)
  /// CRITICAL: This function now throws errors - callers must handle failures
  /// - Throws: MLSError if export fails, validation fails, or upload fails
  public func publishGroupInfo(for userDID: String, convoId: String, groupId: Data, knownServerEpoch: UInt64? = nil) async throws {
    logger.info("📤 [MLSClient.publishGroupInfo] Starting for \(convoId)")

    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] No API client configured for user \(normalizedDID)")
      throw MLSError.configurationError
    }

    // 1. Export GroupInfo from FFI
    // CRITICAL FIX: Use clientIdentity (did#deviceUUID) NOT bare userDID
    // The signer is registered with clientIdentity during key package creation,
    // so we must use the same identity format here to find it.
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    let identityBytes = Data(clientIdentity.utf8)
    let groupInfoBytes = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.exportGroupInfo(groupId: groupId, signerIdentityBytes: identityBytes)
    }

    // 2. Validate exported GroupInfo meets minimum size
    guard groupInfoBytes.count >= Self.minGroupInfoSize else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] Exported GroupInfo too small: \(groupInfoBytes.count) bytes"
      )
      throw MLSError.operationFailed
    }

    // 🔒 FIX #3: Validate GroupInfo format before upload using FFI
    // This catches serialization corruption BEFORE it reaches the server
    let isValid = try await runFFIWithRecovery(for: userDID) { ctx in
      ctx.validateGroupInfoFormat(groupInfoBytes: groupInfoBytes)
    }
    guard isValid else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] GroupInfo validation FAILED - NOT uploading corrupt data")
      logger.error("   Size: \(groupInfoBytes.count) bytes")
      logger.error("   First 32 bytes: \(groupInfoBytes.prefix(32).hexEncodedString())")
      throw MLSError.invalidGroupInfo(
        convoId: convoId,
        message: "Export produced invalid GroupInfo - validation failed before upload")
    }
    logger.info("✅ [MLSClient.publishGroupInfo] GroupInfo validated: \(groupInfoBytes.count) bytes")

    // 3. Get current epoch
    let epoch = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getEpoch(groupId: groupId)
    }

    // 3a. Guard against uploading stale GroupInfo (epoch behind server)
    if let serverEpoch = knownServerEpoch, epoch < serverEpoch {
      logger.warning("⚠️ [MLSClient.publishGroupInfo] Skipping upload - local epoch \(epoch) < server epoch \(serverEpoch) for \(convoId.prefix(16))")
      return
    }

    // 4. Upload to server (MLSAPIClient now has retry logic + verification)
    try await apiClient.updateGroupInfo(
      convoId: convoId, groupInfo: groupInfoBytes, epoch: Int(epoch))

    logger.info(
      "✅ [MLSClient.publishGroupInfo] Success - Published epoch \(epoch), size: \(groupInfoBytes.count) bytes"
    )
  }

  // MARK: - Member Management

  /// Add members to an existing group
  public func addMembers(for userDID: String, groupId: Data, keyPackages: [Data]) async throws
    -> AddMembersResult
  {
    logger.info(
      "📍 [MLSClient.addMembers] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), keyPackages: \(keyPackages.count)"
    )
    guard !keyPackages.isEmpty else {
      logger.error("❌ [MLSClient.addMembers] No key packages provided")
      throw MLSError.operationFailed
    }
    let keyPackageData = keyPackages.map { KeyPackageData(data: $0) }
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.addMembers(groupId: groupId, keyPackages: keyPackageData)
      }
      logger.info(
        "✅ [MLSClient.addMembers] Success - commit: \(result.commitData.count) bytes, welcome: \(result.welcomeData.count) bytes"
      )
      return result
    } catch let error as MlsError {
      // Extract the error message for specific error detection
      let errorMessage: String
      switch error {
      case .InvalidInput(let msg): errorMessage = msg
      case .OpenMlsError(let msg): errorMessage = msg
      default: errorMessage = error.localizedDescription
      }

      logger.error("❌ [MLSClient.addMembers] FAILED: \(errorMessage)")

      // Check for "member already in group" error to enable proper recovery
      // This allows the caller to unreserve key packages and show appropriate UI
      if errorMessage.lowercased().contains("member already in group")
        || errorMessage.lowercased().contains("already in group")
      {
        logger.warning(
          "⚠️ [MLSClient.addMembers] Member already exists - UI may be out of sync with MLS state")
        throw MLSError.memberAlreadyInGroup(member: "unknown")
      }

      throw MLSError.operationFailed
    }
  }

  /// Create a self-update commit to force epoch advancement
  /// This is used to prevent ratchet desynchronization when changing senders
  /// Returns commit data to be sent to server (no welcome for self-updates)
  ///
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - groupId: Group identifier
  /// - Returns: AddMembersResult with commit data (welcomeData will be empty)
  /// - Throws: MLSError if the operation fails
  ///
  /// - Note: After sending commit to server, caller MUST call mergePendingCommit()
  public func selfUpdate(for userDID: String, groupId: Data) async throws -> AddMembersResult {
    logger.info(
      "📍 [MLSClient.selfUpdate] START - user: \(userDID.prefix(20)), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.selfUpdate(groupId: groupId)
      }
      logger.info("✅ [MLSClient.selfUpdate] Success - commit: \(result.commitData.count) bytes")
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.selfUpdate] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Remove members from the group (cryptographically secure)
  /// This creates an MLS commit that advances the epoch and revokes decryption keys
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  ///   - memberIdentities: Array of member credential data (DID bytes) to remove
  /// - Returns: Commit data to send to server
  public func removeMembers(for userDID: String, groupId: Data, memberIdentities: [Data]) async throws
    -> Data
  {
    logger.info(
      "📍 [MLSClient.removeMembers] Removing \(memberIdentities.count) members from group \(groupId.hexEncodedString().prefix(16))"
    )

    do {
      let commitData = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.removeMembers(groupId: groupId, memberIdentities: memberIdentities)
      }

      logger.info(
        "✅ [MLSClient.removeMembers] Success - commit: \(commitData.count) bytes")
      return commitData
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.removeMembers] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Propose adding a member (does not commit)
  /// Use commit_pending_proposals() to commit accumulated proposals
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  ///   - keyPackageData: Serialized key package of member to add
  /// - Returns: ProposeResult with proposal message and reference
  public func proposeAddMember(for userDID: String, groupId: Data, keyPackageData: Data) async throws
    -> ProposeResult
  {
    logger.info(
      "📍 [MLSClient.proposeAddMember] Creating add proposal for group \(groupId.hexEncodedString().prefix(16))"
    )

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.proposeAddMember(groupId: groupId, keyPackageData: keyPackageData)
      }

      logger.info(
        "✅ [MLSClient.proposeAddMember] Success - message: \(result.proposalMessage.count) bytes"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.proposeAddMember] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Propose removing a member (does not commit)
  /// Use commit_pending_proposals() to commit accumulated proposals
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  ///   - memberIdentity: DID bytes of member to remove
  /// - Returns: ProposeResult with proposal message and reference
  public func proposeRemoveMember(for userDID: String, groupId: Data, memberIdentity: Data) async throws
    -> ProposeResult
  {
    logger.info(
      "📍 [MLSClient.proposeRemoveMember] Creating remove proposal for member"
    )

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.proposeRemoveMember(groupId: groupId, memberIdentity: memberIdentity)
      }

      logger.info(
        "✅ [MLSClient.proposeRemoveMember] Success - message: \(result.proposalMessage.count) bytes"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.proposeRemoveMember] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Propose self-update (does not commit)
  /// Use commit_pending_proposals() to commit accumulated proposals
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  /// - Returns: ProposeResult with proposal message and reference
  public func proposeSelfUpdate(for userDID: String, groupId: Data) async throws -> ProposeResult {
    logger.info(
      "📍 [MLSClient.proposeSelfUpdate] Creating self-update proposal for group \(groupId.hexEncodedString().prefix(16))"
    )

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.proposeSelfUpdate(groupId: groupId)
      }

      logger.info(
        "✅ [MLSClient.proposeSelfUpdate] Success - message: \(result.proposalMessage.count) bytes"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.proposeSelfUpdate] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Delete a group from MLS storage
  public func deleteGroup(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.deleteGroup] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.deleteGroup(groupId: groupId)
      }
      logger.info("✅ [MLSClient.deleteGroup] Successfully deleted group")
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.deleteGroup] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Message Encryption/Decryption

  /// Encrypt a message for the group
  public func encryptMessage(for userDID: String, groupId: Data, plaintext: Data) async throws
    -> EncryptResult
  {
    logger.info(
      "📍 [MLSClient.encryptMessage] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), plaintext: \(plaintext.count) bytes"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.encryptMessage(groupId: groupId, plaintext: plaintext)
      }
      logger.info(
        "✅ [MLSClient.encryptMessage] Success - ciphertext: \(result.ciphertext.count) bytes")
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.encryptMessage] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Decrypt a message from the group
  /// Returns the raw DecryptResult from FFI including plaintext, epoch, sequence, and sender credential
  /// Note: For most use cases, prefer MLSCoreContext.shared.decryptAndStore which also persists to database
  public func decryptMessage(
    for userDID: String, groupId: Data, ciphertext: Data, conversationID: String, messageID: String
  ) async throws -> DecryptResult {
    logger.info(
      "📍 [MLSClient.decryptMessage] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), messageID: \(messageID)"
    )

    // ═══════════════════════════════════════════════════════════════════════════
    // EPOCH FENCE PROTOCOL: Ensure in-memory state is synced with disk before decrypt
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the CRITICAL fix for NSE<>App epoch desync. The NSE may have advanced
    // the MLS ratchet on disk while the app was backgrounded. We MUST check and
    // reload before attempting decryption to prevent forward secrecy violations.
    // ═══════════════════════════════════════════════════════════════════════════
    let contextWasReloaded = try await ensureContextSyncedBeforeDecrypt(
      for: userDID,
      groupId: groupId,
      conversationID: conversationID
    )
    if contextWasReloaded {
      logger.info("🔄 [EPOCH FENCE] Context reloaded before decrypt - proceeding with fresh state")
    }

    do {
      // Call FFI directly to get full DecryptResult with sender credential
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.decryptMessage(groupId: groupId, ciphertext: ciphertext)
      }
      
      let gen = MLSCoordinationStore.shared.currentGeneration
      logger.info(
        "✅ Decrypted message [Gen: \(gen)] - epoch: \(result.epoch), seq: \(result.sequenceNumber), plaintext: \(result.plaintext.count) bytes")
      
      // Extract sender DID for logging
      if let senderDID = String(data: result.senderCredential.identity, encoding: .utf8) {
        logger.debug("   Sender: \(senderDID.prefix(24))...")
      }
      
      // ═══════════════════════════════════════════════════════════════════════════
      // Update epoch checkpoint after successful decryption (for next staleness check)
      // ═══════════════════════════════════════════════════════════════════════════
      await MLSEpochCheckpoint.shared.recordEpoch(
        groupId: groupId,
        epoch: result.epoch,
        isNSE: false  // This is the main app
      )

      return result

    } catch let error as MlsError {
      // Extract message from error case
      let errorMessage: String
      switch error {
      case .DecryptionFailed(let msg): errorMessage = msg
      case .OpenMlsError(let msg): errorMessage = msg
      case .InvalidInput(let msg): errorMessage = msg
      default: errorMessage = error.localizedDescription
      }

      let errorMessageLower = errorMessage.lowercased()

      // Detect ratchet state desynchronization errors
      // These can occur when SSE connection fails and client state becomes stale
      if case .DecryptionFailed = error {
        // ANY DecryptionFailed during message processing could indicate state desync
        // OpenMLS errors like RatchetTypeError, InvalidSignature, SecretReuse are all wrapped as DecryptionFailed
        logger.error("🔴 RATCHET STATE DESYNC DETECTED: DecryptionFailed - likely stale MLS state")
        logger.error("   Original error: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(
          message: "DecryptionFailed - MLS state out of sync: \(errorMessage)")
      }

      // Also check message content for specific error keywords
      if errorMessageLower.contains("ratchet") || errorMessageLower.contains("invalidsignature")
        || errorMessageLower.contains("secretreuse") || errorMessageLower.contains("epoch")
      {
        logger.error("🔴 RATCHET STATE DESYNC DETECTED: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(message: errorMessage)
      }

      logger.error("❌ Decryption failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    } catch {
      logger.error("❌ Decryption failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }
  
  // MARK: - Epoch Fence Protocol

  /// Ensure in-memory MLS context is synced with disk before decryption
  /// This is the CRITICAL fix for NSE<>App epoch desync
  /// - Returns: true if context was reloaded (was stale), false if already synced
  private func ensureContextSyncedBeforeDecrypt(
    for userDID: String,
    groupId: Data,
    conversationID: String
  ) async throws -> Bool {
    let normalizedDID = normalizeUserDID(userDID)

    // Get current in-memory epoch for this group
    let inMemoryEpoch: UInt64
    do {
      inMemoryEpoch = try await getEpoch(for: normalizedDID, groupId: groupId)
    } catch {
      // Group doesn't exist in memory - will be initialized during decrypt
      logger.debug(
        "[EPOCH FENCE] No in-memory epoch for group \(conversationID.prefix(16))... - skipping fence"
      )
      return false
    }

    // Check against epoch checkpoint
    guard
      let checkResult = await MLSEpochCheckpoint.shared.checkStaleness(
        groupId: groupId,
        inMemoryEpoch: inMemoryEpoch
      )
    else {
      // No checkpoint exists - first time seeing this group, nothing to check
      logger.debug(
        "[EPOCH FENCE] No checkpoint for group \(conversationID.prefix(16))... - first time")
      return false
    }

    if !checkResult.wasStale {
      // Context is up to date
      logger.debug(
        "[EPOCH FENCE] Context synced for group \(conversationID.prefix(16))... (epoch: \(inMemoryEpoch))"
      )
      return false
    }

    // ⚠️ STALE CONTEXT DETECTED - Must reload before proceeding
    logger.warning(
      "🔄 [EPOCH FENCE] Stale context detected for group \(conversationID.prefix(16))...")
    logger.warning(
      "   In-memory epoch: \(checkResult.memoryEpoch), Disk epoch: \(checkResult.diskEpoch)")
    if let modifiedBy = checkResult.modifiedBy {
      logger.warning("   Last modified by: \(modifiedBy.rawValue)")
    }

    // Reload context from storage
    do {
      let bundleCount = try await reloadContextFromStorage(for: normalizedDID)
      logger.info("✅ [EPOCH FENCE] Context reloaded - bundle count: \(bundleCount)")

      // Verify epoch is now correct
      let newEpoch = try await getEpoch(for: normalizedDID, groupId: groupId)
      if newEpoch >= checkResult.diskEpoch {
        logger.info("✅ [EPOCH FENCE] Epoch now synced: \(newEpoch)")
      } else {
        logger.warning(
          "⚠️ [EPOCH FENCE] Epoch still behind after reload: memory=\(newEpoch), disk=\(checkResult.diskEpoch)"
        )
      }

      return true
    } catch {
      logger.error("❌ [EPOCH FENCE] Failed to reload context: \(error.localizedDescription)")
      // Don't throw - let the decrypt attempt proceed, it may still work or provide better error
      return false
    }
  }

  // MARK: - Key Package Management

  /// Create a key package for this user (low-level with explicit identity)
  /// Use the convenience method without identity parameter for automatic bare DID usage
  public func createKeyPackage(for userDID: String, identity: String) async throws -> Data {
    // NOTE: very noisy in CLI runs
    // logger.info(
    //   "📍 [MLSClient.createKeyPackage] START - user: \(userDID.prefix(20)), identity: \(identity.prefix(30))"
    // )
    // logger.debug("[MLSClient.createKeyPackage] Full userDID: '\(userDID)' (length: \(userDID.count))")
    // logger.debug("[MLSClient.createKeyPackage] Full identity: '\(identity)' (length: \(identity.count))")

    // RECOVERY CHECK: Check if we have a saved identity key in Keychain but not in Rust context
    // This happens on reinstall. If found, import it before creating key package.
    let identityKeyKey = "mls_identity_key_\(userDID)"
    if let savedKeyData = try? MLSKeychainManager.shared.retrieve(forKey: identityKeyKey) {
      let keyData = savedKeyData
      // logger.info("♻️ Found saved identity key in Keychain. Importing to restore identity...")
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.importIdentityKey(identity: identity, keyData: keyData)
        }
        // logger.info("✅ Identity key restored successfully")
      } catch let error as MlsError {
        logger.error("❌ Failed to restore identity key: \(error.localizedDescription)")
        // Continue - will generate new key, but this is suboptimal
      } catch {
        logger.error("❌ Failed to restore identity key: \(error.localizedDescription)")
      }
    }

    let identityBytes = Data(identity.utf8)
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.createKeyPackage(identityBytes: identityBytes)
      }

      // BACKUP: Export and save the identity key to Keychain for future recovery
      if let identityKeyData = try? await runFFIWithRecovery(for: userDID, operation: { ctx in
        try ctx.exportIdentityKey(identity: identity)
      }) {
        try? MLSKeychainManager.shared.store(identityKeyData, forKey: identityKeyKey)
        // logger.debug("💾 Identity key backed up to Keychain for recovery")
      }

      // Log bundle count after creation
      if let bundleCount = try? await runFFIWithRecovery(for: userDID, operation: { ctx in
        try ctx.getKeyPackageBundleCount()
      }) {
        // logger.debug("[MLSClient.createKeyPackage] Bundle count after creation: \(bundleCount)")
      }

      // logger.info("✅ [MLSClient.createKeyPackage] Success - \(result.keyPackageData.count) bytes")
      return result.keyPackageData
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.createKeyPackage] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Create a key package for this user using client identity (did#deviceUUID)
  /// Each device is a unique MLS leaf node for proper multi-device support.
  public func createKeyPackage(for userDID: String) async throws -> Data {
    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.createKeyPackage] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    return try await createKeyPackage(for: userDID, identity: clientIdentity)
  }

  /// Compute the hash reference for a key package
  public func computeKeyPackageHash(for userDID: String, keyPackageData: Data) async throws -> Data {
    logger.debug(
      "📍 [MLSClient.computeKeyPackageHash] Computing hash for \(keyPackageData.count) bytes")
    do {
      let hashBytes = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.computeKeyPackageHash(keyPackageBytes: keyPackageData)
      }
      logger.debug("✅ [MLSClient.computeKeyPackageHash] Hash:  \(hashBytes.hexEncodedString())")
      return hashBytes
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.computeKeyPackageHash] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Get all local key package hashes for a user
  /// Used to verify that local storage matches server inventory
  public func getLocalKeyPackageHashes(for userDID: String) async throws -> [String] {
    logger.debug(
      "📍 [MLSClient.getLocalKeyPackageHashes] Getting local hashes for \(userDID.prefix(20))...")
    do {
      let hashes = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.debugListKeyPackageHashes()
      }
      logger.debug("✅ [MLSClient.getLocalKeyPackageHashes] Found \(hashes.count) local hashes")
      return hashes
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.getLocalKeyPackageHashes] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Update key package for an existing group
  public func updateKeyPackage(for userDID: String, groupId: Data) async throws -> Data {
    logger.error("Update key package not yet implemented in UniFFI API")
    throw MLSError.operationFailed
  }

  // MARK: - Group State

  /// Get the current epoch for a group
  public func getEpoch(for userDID: String, groupId: Data) async throws -> UInt64 {
    do {
      return try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.getEpoch(groupId: groupId)
      }
    } catch let error as MlsError {
      let shouldReload: Bool
      switch error {
      case .GroupNotFound:
        logger.warning(
          "⚠️ [MLSClient.getEpoch] Group not found - attempting context reload before retry")
        shouldReload = true
      case .ContextNotInitialized:
        logger.warning(
          "⚠️ [MLSClient.getEpoch] Context not initialized - attempting context reload before retry"
        )
        shouldReload = true
      case .ContextClosed:
        logger.warning("⚠️ [MLSClient.getEpoch] Context closed - attempting context reload before retry")
        shouldReload = true
      default:
        shouldReload = false
      }

      if shouldReload {
        do {
          _ = try await reloadContextFromStorage(for: userDID)
          return try await runFFIWithRecovery(for: userDID) { ctx in
            try ctx.getEpoch(groupId: groupId)
          }
        } catch {
          logger.error("Get epoch retry failed: \(error.localizedDescription)")
        }
      }

      logger.error("Get epoch failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Get debug information about group members
  public func debugGroupMembers(for userDID: String, groupId: Data) async throws -> GroupDebugInfo {
    do {
      return try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.debugGroupMembers(groupId: groupId)
      }
    } catch let error as MlsError {
      logger.error("Debug group members failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Export a secret from the group's key schedule for debugging/comparison
  /// This can be used to verify that two clients at the same epoch have the same cryptographic state
  public func exportSecret(
    for userDID: String, groupId: Data, label: String, context contextData: Data, keyLength: UInt64
  ) async throws -> Data {
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.exportSecret(
          groupId: groupId, label: label, context: contextData, keyLength: keyLength)
      }
      return result.secret
    } catch let error as MlsError {
      logger.error("Export secret failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Check if a group exists in local storage
  public func groupExists(for userDID: String, groupId: Data) -> Bool {
    (try? getContext(for: userDID).groupExists(groupId: groupId)) ?? false
  }

  /// Get group info for external parties
  public func getGroupInfo(for userDID: String, groupId: Data) async throws -> Data {
    logger.error("Get group info not yet implemented in UniFFI API")
    throw MLSError.operationFailed
  }

  /// Process a commit message
  public func processCommit(for userDID: String, groupId: Data, commitData: Data) async throws
    -> ProcessCommitResult
  {
    logger.info(
      "📍 [MLSClient.processCommit] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), commit: \(commitData.count) bytes"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.processCommit(groupId: groupId, commitData: commitData)
      }
      logger.info(
        "✅ [MLSClient.processCommit] Success - newEpoch: \(result.newEpoch), updateProposals: \(result.updateProposals.count)"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.processCommit] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Create a commit for pending proposals
  public func createCommit(for userDID: String, groupId: Data) async throws -> Data {
    logger.error("Create commit not yet implemented in UniFFI API")
    throw MLSError.operationFailed
  }

  /// Clear pending commit for a group
  public func clearPendingCommit(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.clearPendingCommit] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.clearPendingCommit(groupId: groupId)
      }
      logger.info("✅ [MLSClient.clearPendingCommit] Success")
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.clearPendingCommit] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Merge a pending commit after validation
  public func mergePendingCommit(for userDID: String, groupId: Data, convoId: String? = nil) async throws
    -> UInt64
  {
    logger.info(
      "📍 [MLSClient.mergePendingCommit] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      let newEpoch = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.mergePendingCommit(groupId: groupId)
      }
      logger.info("✅ [MLSClient.mergePendingCommit] Success - newEpoch: \(newEpoch)")

      // If convoId is provided, publish the new GroupInfo
      // CRITICAL: Now awaited instead of fire-and-forget
      if let convoId = convoId {
        try await self.publishGroupInfo(for: userDID, convoId: convoId, groupId: groupId)
      }

      return newEpoch
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.mergePendingCommit] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Merge a staged commit after validation
  public func mergeStagedCommit(for userDID: String, groupId: Data) async throws -> UInt64 {
    do {
      let newEpoch = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.mergeStagedCommit(groupId: groupId)
      }
      logger.info("Staged commit merged, new epoch: \(newEpoch)")
      return newEpoch
    } catch let error as MlsError {
      logger.error("Merge staged commit failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Proposal Inspection and Management

  /// Process a message and return detailed information about its content
  public func processMessage(for userDID: String, groupId: Data, messageData: Data) async throws
    -> ProcessedContent
  {
    // CRITICAL FIX: Strip padding envelope before MLS deserialization
    // Messages may be padded to bucket sizes (512, 1024, etc.) for traffic analysis resistance.
    // Format: [4-byte BE length][actual MLS ciphertext][zero padding...]
      let actualMessageData = try MLSMessagePadding.removePadding(messageData)
    
    if actualMessageData.count != messageData.count {
      logger.info(
        "📍 [MLSClient.processMessage] Stripped padding: \(messageData.count) -> \(actualMessageData.count) bytes"
      )
    }
    
    logger.info(
      "📍 [MLSClient.processMessage] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), message: \(actualMessageData.count) bytes"
    )
    // ═══════════════════════════════════════════════════════════════════════════
    // EPOCH FENCE PROTOCOL: Ensure in-memory state is synced with disk before process
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the CRITICAL fix for NSE<>App epoch desync. The NSE may have advanced
    // the MLS ratchet on disk while the app was backgrounded. We MUST check and
    // reload before attempting message processing to prevent forward secrecy violations.
    // ═══════════════════════════════════════════════════════════════════════════
    let contextWasReloaded = try await ensureContextSyncedBeforeDecrypt(
      for: userDID,
      groupId: groupId,
      conversationID: "push-message"  // We don't have convoID here, but logging inside handles checking
    )
    if contextWasReloaded {
      logger.info(
        "🔄 [EPOCH FENCE] Context reloaded before processMessage - proceeding with fresh state")
    }

    do {
      let content = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.processMessage(groupId: groupId, messageData: actualMessageData)
      }
      
      // ═══════════════════════════════════════════════════════════════════════════
      // Update epoch checkpoint after successful processing (for next staleness check)
      // ═══════════════════════════════════════════════════════════════════════════
      // Note: We need to get the new epoch to update the checkpoint.
      // process_message returns ProcessedContent which wraps the ApplicationMessage
      // It doesn't explicitly return the new epoch, but if successful, we are at the epoch
      // of the message. We'll fetch the current epoch to be safe.

      // Perform async epoch fetch to update checkpoint without blocking return
      Task {
        do {
          let newEpoch = try await self.getEpoch(for: userDID, groupId: groupId)
          await MLSEpochCheckpoint.shared.recordEpoch(
            groupId: groupId,
            epoch: newEpoch,
            isNSE: false
          )
        } catch {
          self.logger.warning(
            "⚠️ [MLSClient.processMessage] Failed to update epoch checkpoint: \(error.localizedDescription)"
          )
        }
      }

      logger.info(
        "✅ [MLSClient.processMessage] Success - content type: \(String(describing: content))")
      return content
    } catch let error as MlsError {
      // Extract message from error case
      // NOTE: UniFFI generates two different Swift cases for OpenMLS errors:
      //   - .OpenMlsError - for Rust MLSError::OpenMLSError (unit variant, generic message)
      //   - .OpenMls - for Rust MLSError::OpenMLS(String) (has detailed error message)
      let errorMessage: String
      switch error {
      case .DecryptionFailed(let msg): errorMessage = msg
      case .OpenMlsError(let msg): errorMessage = msg
      case .OpenMls(let msg): errorMessage = msg  // Detailed OpenMLS errors with context
      case .InvalidInput(let msg): errorMessage = msg
      default: errorMessage = error.localizedDescription
      }

      let errorMessageLower = errorMessage.lowercased()

      // Check for "old epoch" error which is safe to ignore for new joiners
      // This happens when the server sends the Commit message that added us, but we joined via Welcome (already at new epoch)
      // Also check for OpenMLS epoch-related errors like "EpochMismatch", "WrongEpoch", "ValidationError"
      if errorMessage.contains("Cannot decrypt message from epoch") 
         || errorMessageLower.contains("epochmismatch")
         || errorMessageLower.contains("wrong epoch")
         || errorMessageLower.contains("wrongepoch")
         || (errorMessageLower.contains("validation") && errorMessageLower.contains("epoch")) {
        logger.warning("⚠️ Ignoring message from old epoch: \(errorMessage)")
        throw MLSError.ignoredOldEpochMessage
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2024-12): Handle SecretReuseError as a skip, NOT a desync
      // ═══════════════════════════════════════════════════════════════════════════
      //
      // Problem: SecretReuseError occurs when the same message is decrypted twice.
      // This commonly happens when:
      // 1. NSE decrypts a message (advances ratchet, deletes key)
      // 2. Main app tries to decrypt the same message (key is gone)
      //
      // Old behavior: Treated as ratchetStateDesync → triggers group rejoin
      // New behavior: Treat as secretReuseSkipped → caller should check DB cache
      //
      // This is NOT a true desync - the message WAS decrypted successfully (by NSE).
      // The plaintext should be in the database cache.
      //
      // ═══════════════════════════════════════════════════════════════════════════
      if errorMessageLower.contains("secretreuse") || errorMessageLower.contains("secret_reuse")
         || errorMessage.contains("SecretReuseError") || errorMessage.contains("SecretTreeError(SecretReuseError)")
      {
        logger.info("ℹ️ [MLSClient.processMessage] SecretReuseError - message already decrypted (likely by NSE)")
        logger.info("   This is expected when NSE and main app race to decrypt the same message")
        logger.info("   Caller should retrieve plaintext from database cache")
        // Note: We don't have messageID here, but caller will handle appropriately
        throw MLSError.secretReuseSkipped(messageID: "unknown")
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2026-01): Handle CannotDecryptOwnMessage BEFORE ratchetStateDesync
      // ═══════════════════════════════════════════════════════════════════════════
      // This error occurs when we try to decrypt a message we sent ourselves.
      // MLS encrypts messages for recipients only - senders can't decrypt their own messages.
      // This is NOT a desync - it's a permanent failure that retrying won't fix.
      // The caller should use the pre-cached payload from when the message was sent.
      // ═══════════════════════════════════════════════════════════════════════════
      if errorMessage.contains("CannotDecryptOwnMessage") || errorMessageLower.contains("cannotdecryptownmessage") {
        logger.warning("⚠️ [MLSClient.processMessage] CannotDecryptOwnMessage - this is a self-sent message")
        logger.info("   MLS messages are encrypted for recipients only, senders cannot decrypt their own messages")
        logger.info("   Caller should use pre-cached payload from send operation")
        throw MLSError.cannotDecryptOwnMessage
      }

      // Detect ratchet state desynchronization errors
      // These can occur when SSE connection fails and client state becomes stale
      if case .DecryptionFailed = error {
        // DecryptionFailed OTHER than SecretReuseError indicates true state desync
        // OpenMLS errors like RatchetTypeError, InvalidSignature are wrapped as DecryptionFailed
        logger.error(
          "🔴 RATCHET STATE DESYNC DETECTED in processMessage: DecryptionFailed - likely stale MLS state"
        )
        logger.error("   Original error: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(
          message: "DecryptionFailed - MLS state out of sync: \(errorMessage)")
      }
      
      // Check OpenMlsError case (generic, unit variant from Rust)
      if case .OpenMlsError = error {
        logger.error(
          "🔴 OpenMLS error in processMessage: \(errorMessage)"
        )
        logger.error("   This may indicate protocol issues or state desync")
        // Check if it's an epoch-related or decryption error
        if errorMessageLower.contains("decrypt") || errorMessageLower.contains("epoch") 
           || errorMessageLower.contains("ratchet") || errorMessageLower.contains("signature") {
          throw MLSError.ratchetStateDesync(
            message: "OpenMlsError - MLS state issue: \(errorMessage)")
        }
        // For other OpenMLS errors, throw with the message
        throw MLSError.invalidContent("OpenMLS error: \(errorMessage)")
      }
      
      // Check OpenMls case (detailed errors with context from Rust MLSError::OpenMLS(String))
      // This is the variant used by process_message failures with detailed error info
      if case .OpenMls = error {
        logger.error(
          "🔴 OpenMLS detailed error in processMessage: \(errorMessage)"
        )
        logger.error("   This contains detailed error context from the Rust FFI")
        // Check if it's an epoch-related or decryption error
        if errorMessageLower.contains("decrypt") || errorMessageLower.contains("epoch") 
           || errorMessageLower.contains("ratchet") || errorMessageLower.contains("signature")
           || errorMessageLower.contains("secrettreerror") || errorMessageLower.contains("secret_tree") {
          throw MLSError.ratchetStateDesync(
            message: "OpenMLS - MLS state issue: \(errorMessage)")
        }
        // For other OpenMLS errors, throw with the detailed message
        throw MLSError.invalidContent("OpenMLS error: \(errorMessage)")
      }

      // Also check message content for specific error keywords (excluding SecretReuse which is handled above)
      if errorMessageLower.contains("ratchet") || errorMessageLower.contains("invalidsignature")
        || errorMessageLower.contains("epoch")
      {
        logger.error("🔴 RATCHET STATE DESYNC DETECTED in processMessage: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(message: errorMessage)
      }

      logger.error("❌ [MLSClient.processMessage] FAILED: \(error.localizedDescription)")
      logger.error("   Error type: \(type(of: error)), error case: \(error)")
      logger.error("   Error message extracted: \(errorMessage)")
      throw MLSError.operationFailed
    }
  }

  /// Store a validated proposal in the proposal queue
  public func storeProposal(for userDID: String, groupId: Data, proposalRef: ProposalRef) async throws {
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.storeProposal(groupId: groupId, proposalRef: proposalRef)
      }
      logger.info("Proposal stored successfully")
    } catch let error as MlsError {
      logger.error("Store proposal failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// List all pending proposals for a group
  public func listPendingProposals(for userDID: String, groupId: Data) async throws -> [ProposalRef] {
    do {
      let proposals = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.listPendingProposals(groupId: groupId)
      }
      logger.info("Found \(proposals.count) pending proposals")
      return proposals
    } catch let error as MlsError {
      logger.error("List proposals failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Remove a proposal from the proposal queue
  public func removeProposal(for userDID: String, groupId: Data, proposalRef: ProposalRef) async throws {
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.removeProposal(groupId: groupId, proposalRef: proposalRef)
      }
      logger.info("Proposal removed successfully")
    } catch let error as MlsError {
      logger.error("Remove proposal failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Commit all pending proposals that have been validated
  public func commitPendingProposals(for userDID: String, groupId: Data) async throws -> Data {
    do {
      let commitData = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.commitPendingProposals(groupId: groupId)
      }
      logger.info("Pending proposals committed successfully")
      return commitData
    } catch let error as MlsError {
      logger.error("Commit proposals failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Persistence

  /// Phase 4: Monitor and automatically replenish key package bundles
  /// Proactively checks server inventory and uploads bundles when running low
  /// - Parameter userDID: User DID to monitor bundles for
  /// - Returns: Tuple of (available bundles on server, bundles uploaded)
  public func monitorAndReplenishBundles(for userDID: String) async throws -> (
    available: Int, uploaded: Int
  ) {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [Phase 4] API client not configured for user \(normalizedDID) - cannot monitor bundles")
      throw MLSError.operationFailed
    }

    logger.info(
      "🔍 [Phase 4] Starting proactive bundle monitoring for user: \(userDID.prefix(20))...")

    // CRITICAL: Check local bundles FIRST before querying server
    // This catches the desync case where local=0 but server>0
    var localBundleCount: UInt64 = 0
    do {
      localBundleCount = try await ensureLocalBundlesAvailable(for: userDID)
      logger.info("📍 [Phase 4] Local bundle count: \(localBundleCount)")
    } catch {
      logger.warning("⚠️ [Phase 4] Failed to check local bundles: \(error.localizedDescription)")
    }

    // Query server bundle status (Phase 3 endpoint)
    let status = try await apiClient.getKeyPackageStatus()

    // Detect and handle local=0, server>0 desync
    if localBundleCount == 0 && status.available > 0 {
      logger.warning("⚠️ [Phase 4] DESYNC DETECTED: Local=0, Server=\(status.available)")
      logger.info("   🔄 Attempting non-destructive context reload...")

      do {
        let recoveredCount = try await reloadContextFromStorage(for: userDID)
        if recoveredCount > 0 {
          logger.info("   ✅ Recovered \(recoveredCount) bundles from storage reload!")
          localBundleCount = recoveredCount
        } else {
          logger.warning("   ⚠️ Storage reload found 0 bundles - will need reconciliation")
        }
      } catch {
        logger.error("   ❌ Context reload failed: \(error.localizedDescription)")
      }
    }

    logger.info("📊 [Phase 4] Server bundle status:")
    logger.debug("   - Total uploaded: \(status.totalUploaded)")
    logger.debug("   - Available: \(status.available)")
    logger.debug("   - Consumed: \(status.consumed)")
    logger.debug("   - Reserved: \(String(describing:status.reserved))")

    // Detect recovery mode condition: both local and server have 0 bundles for this device
    // This indicates a fresh install or corrupted state that needs rate limit bypass
    let useRecoveryMode = localBundleCount == 0 && status.available == 0
    if useRecoveryMode {
      logger.warning("🔑 [Phase 4] Recovery mode detected: local=0, server=0 - will bypass rate limits")
    }

    // Configuration for bundle replenishment
    let minimumAvailableBundles = 10
    // In recovery mode, limit to 50 bundles (server's MAX_RECOVERY_BATCH)
    let targetBundleCount = useRecoveryMode ? 25 : 25
    let batchUploadSize = 5

    // Check if replenishment is needed
    if status.available >= minimumAvailableBundles {
      logger.info(
        "✅ [Phase 4] Sufficient bundles available (\(status.available)) - no action needed")
      return (available: status.available, uploaded: 0)
    }

    // Calculate how many bundles to upload
    let neededCount = targetBundleCount - status.available
    logger.warning(
      "⚠️ [Phase 4] Low bundle count! Available: \(status.available), minimum: \(minimumAvailableBundles)"
    )
    logger.info(
      "🔧 [Phase 4] Replenishing \(neededCount) bundles to reach target of \(targetBundleCount)")

    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [Phase 4] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }

    // Get device ID for server-side deduplication
    let deviceInfo = await getDeviceInfo(for: userDID)
    let deviceId = deviceInfo?.deviceId

    // Create and upload bundles in batches
    var uploadedCount = 0

    for batchIndex in stride(from: 0, to: neededCount, by: batchUploadSize) {
      let batchCount = min(batchUploadSize, neededCount - batchIndex)
      logger.debug(
        "📦 [Phase 4] Creating batch \(batchIndex/batchUploadSize + 1) - \(batchCount) bundles")

      var batchPackages: [MLSKeyPackageUploadData] = []

      for i in 0..<batchCount {
        do {
          let keyPackageBytes = try await createKeyPackage(for: userDID, identity: clientIdentity)
          let base64Package = keyPackageBytes.base64EncodedString()
          let idempotencyKey = UUID().uuidString.lowercased()

          batchPackages.append(
            MLSKeyPackageUploadData(
              keyPackage: base64Package,
              cipherSuite: "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519",
              expires: Date().addingTimeInterval(90 * 24 * 60 * 60),  // 90 days
              idempotencyKey: idempotencyKey,
              deviceId: deviceId,
              credentialDid: clientIdentity
            ))

          logger.debug("   ✅ Created bundle \(batchIndex + i + 1)/\(neededCount)")
        } catch {
          logger.error(
            "   ❌ Failed to create bundle \(batchIndex + i + 1): \(error.localizedDescription)")
          throw error
        }
      }

      // Upload batch to server (use recovery mode to bypass rate limits if device has 0 key packages)
      do {
        let result = try await apiClient.publishKeyPackagesBatch(batchPackages, recoveryMode: useRecoveryMode)
        logger.debug(
          "   📤 Batch upload complete - succeeded: \(result.succeeded), failed: \(result.failed)")

        if let errors = result.errors, !errors.isEmpty {
          logger.warning("   ⚠️ Some uploads failed:")
          for error in errors {
            logger.debug("      - Index \(error.index): \(error.error)")
          }
        }

        uploadedCount += result.succeeded
      } catch {
        logger.error("   ❌ Batch upload failed: \(error.localizedDescription)")
        throw error
      }

      // Small delay between batches to avoid overwhelming server
      if batchIndex + batchUploadSize < neededCount {
        try await Task.sleep(for: .milliseconds(100))
      }
    }

    logger.info("✅ [Phase 4] Replenishment complete - uploaded \(uploadedCount) bundles")
    logger.info("📊 [Phase 4] New server bundle count: \(status.available + uploadedCount)")

    return (available: status.available + uploadedCount, uploaded: uploadedCount)
  }

  /// Phase 4: Diagnostic logging for bundle lifecycle
  /// Logs comprehensive bundle state for debugging
  public func logBundleDiagnostics(for userDID: String) async throws {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [Phase 4] API client not configured for user \(normalizedDID) - cannot run diagnostics")
      throw MLSError.operationFailed
    }

    logger.info("🔬 [Phase 4] Bundle Diagnostics for user: \(userDID.prefix(20))")

    // Local bundle count (Phase 2 FFI query)
    let localCount: UInt64
    do {
      localCount = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.getKeyPackageBundleCount()
      }
      logger.info("   📍 Local bundles in cache: \(localCount)")
    } catch {
      logger.warning("   ⚠️ Failed to query local bundles: \(error.localizedDescription)")
      throw error
    }

    // Server bundle status (Phase 3 endpoint)
    do {
      let status = try await apiClient.getKeyPackageStatus(limit: 5)
      logger.info("   📍 Server bundle status:")
      logger.info("      - Total uploaded: \(status.totalUploaded)")
      logger.info("      - Available: \(status.available)")
      logger.info("      - Consumed: \(status.consumed)")
      logger.info("      - Reserved: \(status.reserved ?? 0)")

      if let consumedPackages = status.consumedPackages, !consumedPackages.isEmpty {
        logger.debug("   📜 Recent consumption history (last \(consumedPackages.count)):")
        for pkg in consumedPackages {
          logger.debug(
            "      - Hash: \(pkg.keyPackageHash.prefix(16))... | Consumed: \(pkg.consumedAt.date) | Group: \(pkg.usedInGroup ?? "unknown")"
          )
        }
      }

      // Warning thresholds
      let minimumAvailableBundles = 10
      if status.available < minimumAvailableBundles {
        logger.warning(
          "   ⚠️ ALERT: Available bundles (\(status.available)) below minimum threshold (\(minimumAvailableBundles))"
        )
        logger.warning("      ACTION REQUIRED: Call monitorAndReplenishBundles() to replenish")
      }

      if status.available == 0 {
        logger.error("   🚨 CRITICAL: No bundles available! Cannot process Welcome messages!")
      }
    } catch {
      logger.error("   ❌ Failed to query server status: \(error.localizedDescription)")
      throw error
    }

    logger.info("✅ [Phase 4] Diagnostics complete")
  }

  /// Verify that local key packages exist in SQLite storage
  /// With automatic SQLite persistence, bundles should exist after initial creation
  /// Returns the number of local bundles available
  public func ensureLocalBundlesAvailable(for userDID: String) async throws -> UInt64 {
    let bundleCount = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getKeyPackageBundleCount()
    }

    if bundleCount == 0 {
      logger.warning("⚠️ No local bundles found in SQLite storage for user: \(userDID.prefix(20))")
      logger.warning("   This may indicate first use or post-logout state")
      logger.warning(
        "   Consider calling monitorAndReplenishBundles() to generate and upload bundles")
    } else {
      logger.debug("✅ Found \(bundleCount) local bundles in SQLite storage")
    }

    return bundleCount
  }

  /// Get the current key package bundle count for a user
  /// Used by recovery manager to check for desync
  public func getKeyPackageBundleCount(for userDID: String) async throws -> UInt64 {
    try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getKeyPackageBundleCount()
    }
  }

  /// Setup lifecycle observers for automatic storage persistence
  ///
  /// Note: MLSClient is a singleton that manages multiple per-user MLS contexts.
  /// Lifecycle observers (app backgrounding, termination) should be handled by
  /// AppState or AuthManager which knows the currently active user, then call
  /// flushStorage(for:) on the appropriate user's context.
  ///
  /// This approach is intentional to maintain clean separation between the
  /// crypto layer (MLSClient) and app state management.
  private func setupLifecycleObservers() {
    // Intentionally empty - see note above
  }

  /// Force flush all pending database writes to disk for a specific user
  ///
  /// This executes a SQLite WAL checkpoint to ensure all pending writes are
  /// durably persisted to the main database file. The Rust FFI layer now
  /// auto-flushes after each key package creation, but this method can be
  /// called explicitly after batch operations for extra safety.
  ///
  /// - Parameter userDID: The user's DID
  /// - Throws: MLSError if flush fails
  public func flushStorage(for userDID: String) async throws {
    let normalizedDID = normalizeUserDID(userDID)

    try await withMLSUserPermit(for: normalizedDID) {
      // No advisory lock needed - SQLite WAL handles concurrent access
      // Cross-process coordination uses Darwin notifications (MLSCrossProcess)

      try await self.flushStorageLocked(normalizedDID: normalizedDID)
    }
  }

  private func flushStorageLocked(normalizedDID: String) async throws {
    logger.info("💾 Flushing MLS storage for user: \(normalizedDID.prefix(20))")

    do {
      try await runFFIWithRecoveryLocked(for: normalizedDID) { ctx in
        try ctx.flushStorage()
      }
      logger.info("✅ MLS storage flushed successfully")
    } catch let error as MlsError {
      logger.error("❌ Failed to flush MLS storage: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }
  
  /// Close and release an MLS context for a specific user
  ///
  /// CRITICAL: Call this during account switching to prevent SQLite connection exhaustion.
  /// This method:
  /// 1. Flushes all pending writes to disk (WAL checkpoint)
  /// 2. Removes the context from the in-memory cache
  /// 3. Removes associated API clients and managers
  ///
  /// The underlying Rust FFI context will be deallocated when all Arc references are dropped.
  /// SQLite connections are closed when the rusqlite::Connection is dropped.
  ///
  /// - Parameter userDID: The user's DID to close context for
  /// - Returns: True if a context was closed, false if no context existed for this user
  @discardableResult
  public func closeContext(for userDID: String) async -> Bool {
    let normalizedDID = normalizeUserDID(userDID)
    bumpGeneration(for: normalizedDID)

    do {
      return try await withMLSUserPermit(for: normalizedDID) {
        // No advisory lock needed - SQLite WAL handles concurrent access
        // Cross-process coordination uses Darwin notifications (MLSCrossProcess)

        return await self.closeContextLocked(normalizedDID: normalizedDID)
      }
    } catch {
      logger.error("🚨 [MLSClient] Failed to acquire permit for closeContext: \(error.localizedDescription)")
      return false
    }
  }

  private func closeContextLocked(normalizedDID: String) async -> Bool {
    logger.info("🛑 [MLSClient] Closing context for user: \(normalizedDID.prefix(20))...")

    // Try to flush before closing (but don't fail if flush fails)
    if contexts[normalizedDID] != nil {
      do {
        try await runFFIWithRecoveryLocked(for: normalizedDID) { ctx in
          try ctx.flushAndPrepareClose()
        }
        logger.debug("   ✅ Context flushed before close")
      } catch {
        logger.warning("   ⚠️ Flush before close failed: \(error.localizedDescription)")
      }
    }

    let hadContext = contexts.removeValue(forKey: normalizedDID) != nil

    apiClients.removeValue(forKey: normalizedDID)
    deviceManagers.removeValue(forKey: normalizedDID)
    recoveryManagers.removeValue(forKey: normalizedDID)

    if hadContext {
      logger.info("   ✅ Context closed and removed from cache")
    } else {
      logger.debug("   ℹ️ No context existed for this user")
    }

    return hadContext
  }
  
  /// Close all contexts except for the specified user
  ///
  /// CRITICAL: Call this during account switching to prevent SQLite connection exhaustion.
  /// This closes all contexts for other users, preventing "out of memory" errors from
  /// accumulated SQLite connections.
  ///
  /// - Parameter keepUserDID: The user DID to keep open (the active user after switch)
  /// - Returns: Number of contexts that were closed
  @discardableResult
  public func closeAllContextsExcept(keepUserDID: String) async -> Int {
    let normalizedKeepDID = normalizeUserDID(keepUserDID)
    logger.info("🧹 [MLSClient] Closing all contexts except: \(normalizedKeepDID.prefix(20))...")
    
    let usersToClose = contexts.keys.filter { $0 != normalizedKeepDID }
    var closedCount = 0
    
    for userDID in usersToClose {
      if await closeContext(for: userDID) {
        closedCount += 1
      }
    }
    
    logger.info("   ✅ Closed \(closedCount) context(s), kept context for \(normalizedKeepDID.prefix(20))")
    return closedCount
  }

  /// Clear all MLS storage for a specific user.
  ///
  /// IMPORTANT: This is a manual, user-initiated operation. It quarantines files (does not delete).
  public func clearStorage(for userDID: String) async throws {
    let normalizedDID = normalizeUserDID(userDID)
    logger.info("🧰 [Diagnostics] Resetting MLS storage for user: \(normalizedDID)")

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 1: Signal globally that this user is under maintenance
    // ═══════════════════════════════════════════════════════════════════════════
    // This tells ALL processes (main app, NSE) to stop touching this user's data.
    // We increment generation to invalidate any in-flight tasks.
    // ═══════════════════════════════════════════════════════════════════════════
    MLSAppActivityState.setShuttingDown(true, userDID: normalizedDID)
    MLSCoordinationStore.shared.incrementGeneration(for: normalizedDID)
    bumpGeneration(for: normalizedDID)

    // Ensure we clear the shutdown flag when done
    defer {
      MLSAppActivityState.setShuttingDown(false, userDID: normalizedDID)
    }
    
    if let coordinator = storageMaintenanceCoordinator {
      await coordinator.beginStorageMaintenance(for: normalizedDID)
      defer {
        Task { await coordinator.endStorageMaintenance(for: normalizedDID) }
      }
      await coordinator.prepareMLSStorageReset(for: normalizedDID)
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 2: Wait for in-flight operations to notice and cancel
    // ═══════════════════════════════════════════════════════════════════════════
    // Give any active operations a moment to check their generation and bail out.
    // This is a safety buffer, not a hard synchronization point.
    // ═══════════════════════════════════════════════════════════════════════════
    try? await Task.sleep(nanoseconds: 300_000_000)  // 300ms

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 3: Close the database gate and wait for drain
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the authoritative point where we block new connections and wait
    // for existing ones to finish. If drain times out, we force close.
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      try await MLSDatabaseGate.shared.closeGateAndDrain(for: normalizedDID, timeout: .seconds(5))
      logger.info("✅ [Diagnostics] Database gate closed and drained")
    } catch {
      logger.warning(
        "⚠️ [Diagnostics] Gate drain timed out - force closing: \(error.localizedDescription)")
      await MLSDatabaseGate.shared.forceCloseGate(for: normalizedDID)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 4: Perform the actual reset
    // ═══════════════════════════════════════════════════════════════════════════

    // Drop in-memory Rust context so it will reload from disk on next operation.
    contexts.removeValue(forKey: normalizedDID)

    // Quarantine + reset the Swift SQLCipher database.
    try await MLSGRDBManager.shared.quarantineAndResetDatabase(for: normalizedDID)

    // Quarantine the Rust SQLite file (mls-state) so it can be recreated fresh.
    let appSupport: URL
    if let sharedContainer = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared")
    {
      appSupport = sharedContainer
    } else {
      appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
    }

    let mlsStateDir = appSupport.appendingPathComponent("mls-state", isDirectory: true)

    let didHash = normalizedDID.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(64) ?? "default"

    let storageFileURL = mlsStateDir.appendingPathComponent("\(didHash).db")
    let wal = storageFileURL.appendingPathExtension("wal")
    let shm = storageFileURL.appendingPathExtension("shm")

    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withDashSeparatorInDate, .withColonSeparatorInTime]
    let timestamp = formatter.string(from: Date())

    let quarantineDir = mlsStateDir
      .appendingPathComponent("Quarantine", isDirectory: true)
      .appendingPathComponent("\(timestamp)_\(didHash.prefix(16))", isDirectory: true)

    try? FileManager.default.createDirectory(at: quarantineDir, withIntermediateDirectories: true)

    for url in [storageFileURL, wal, shm] {
      guard FileManager.default.fileExists(atPath: url.path) else { continue }
      let dest = quarantineDir.appendingPathComponent(url.lastPathComponent)
      do {
        try FileManager.default.moveItem(at: url, to: dest)
        logger.info("📦 [Diagnostics] Quarantined Rust storage file: \(url.lastPathComponent)")
      } catch {
        logger.warning("⚠️ [Diagnostics] Failed to quarantine \(url.lastPathComponent): \(error.localizedDescription)")
      }
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 5: Reopen the gate for fresh operations
    // ═══════════════════════════════════════════════════════════════════════════
    await MLSDatabaseGate.shared.openGate(for: normalizedDID)

    logger.info("✅ [Diagnostics] MLS storage reset complete for \(normalizedDID)")
  }

  /// Delete specific consumed key package bundles from storage
  ///
  /// Removes bundles that the server has marked as consumed but remain in local storage.
  /// This prevents the "local 101 vs server 47" desync issue without requiring full re-registration.
  ///
  /// - Parameters:
  ///   - userDID: User DID
  ///   - hashRefs: Array of hash references to delete (from server's consumedPackages)
  /// - Returns: Number of bundles successfully deleted
  /// - Throws: MLSError if deletion fails
  public func deleteKeyPackageBundles(for userDID: String, hashRefs: [Data]) async throws -> UInt64
  {
    let normalizedDID = normalizeUserDID(userDID)

    guard !hashRefs.isEmpty else {
      logger.debug("No key package bundles to delete")
      return 0
    }

    logger.info(
      "🗑️ Deleting \(hashRefs.count) consumed key package bundles for \(normalizedDID.prefix(20))..."
    )

    // Call Rust FFI method to delete from both in-memory and persistent storage
    // hashRefs is already [Data], which UniFFI will convert to Vec<Vec<u8>>
    let deletedCount = try await runFFIWithRecovery(for: normalizedDID) { ctx in
      try ctx.deleteKeyPackageBundles(hashRefs: hashRefs)
    }

    logger.info("✅ Deleted \(deletedCount) bundles from storage")

    return deletedCount
  }

  // MARK: - Server Reconciliation (Phase 2)

  /// Reconcile local key package bundles with server inventory
  /// Detects storage corruption and server-client desync
  /// Should be called during app launch after storage load
  /// - Parameter userDID: User DID to reconcile bundles for
  /// - Returns: Tuple of (server available count, local bundle count, desync detected)
  public func reconcileKeyPackagesWithServer(for userDID: String) async throws -> (
    serverAvailable: Int, localBundles: Int, desyncDetected: Bool
  ) {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [Reconciliation] API client not configured for user \(normalizedDID) - cannot reconcile")
      throw MLSError.configurationError
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MULTI-ACCOUNT FIX: Skip reconciliation if this account is not active
    // ═══════════════════════════════════════════════════════════════════════════
    // In multi-account scenarios, the ATProtoClient's getDid() returns whichever
    // account is currently active - NOT necessarily the account we're reconciling for.
    // Instead of throwing an error, gracefully skip reconciliation for inactive accounts.
    // ═══════════════════════════════════════════════════════════════════════════
    let isActiveAccount = await apiClient.isAuthenticatedAs(userDID)
    if !isActiveAccount {
      // This is normal in multi-account scenarios - just skip silently
      logger.info("⏸️ [Reconciliation] Skipping - this account (\(userDID.prefix(20))...) is not the active account")
      return (serverAvailable: 0, localBundles: 0, desyncDetected: false)
    }

    logger.info(
      "🔍 [Reconciliation] Starting key package reconciliation for user: \(userDID.prefix(20))...")

    // Query local bundle count
    var localCount: Int
    do {
      localCount = Int(
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.getKeyPackageBundleCount()
        })
      logger.info("📍 [Reconciliation] Local bundles in cache: \(localCount)")
    } catch {
      logger.error(
        "❌ [Reconciliation] Failed to query local bundle count: \(error.localizedDescription)")
      throw error
    }

    // SQLite storage is automatic - no need to manually load/hydrate

    // Query server bundle inventory
    var serverStats: BlueCatbirdMlsGetKeyPackageStats.Output
    do {
      serverStats = try await apiClient.getKeyPackageStats()
      logger.info("📍 [Reconciliation] Server bundle status:")
      logger.info("   - Available: \(serverStats.available)")
      logger.info("   - Threshold: \(serverStats.threshold)")
    } catch {
      logger.error(
        "❌ [Reconciliation] Failed to query server inventory: \(error.localizedDescription)")
      throw error
    }

    var desyncDetected = localCount != serverStats.available

    if desyncDetected {
      logger.error("🚨 [Reconciliation] KEY PACKAGE DESYNC DETECTED!")
      logger.error("   Local storage: \(localCount) bundles")
      logger.error("   Server inventory: \(serverStats.available) bundles")
      logger.error("   Difference: \(abs(localCount - serverStats.available)) bundles")

      if localCount == 0 && serverStats.available > 0 {
        logger.error(
          "   ❌ STORAGE CORRUPTION SUSPECTED: Local storage empty but server has \(serverStats.available) bundles"
        )

        // Double-check server inventory before any recovery action
        do {
          let confirmationStats = try await apiClient.getKeyPackageStats()
          logger.info(
            "   📍 [Reconciliation] Confirmation check - server available: \(confirmationStats.available)"
          )
          serverStats = confirmationStats
        } catch {
          logger.warning("   ⚠️ Confirmation check failed: \(error.localizedDescription)")
        }

        if serverStats.available == 0 {
          logger.info("   ✅ Server inventory drained during confirmation - skipping recovery")
          desyncDetected = localCount != serverStats.available
        } else {
          // PHASE 1: Try non-destructive recovery first by reloading context from SQLite
          logger.info("   🔄 [Phase 1] Attempting non-destructive context reload...")
          do {
            let recoveredCount = try await reloadContextFromStorage(for: userDID)
            if recoveredCount > 0 {
              logger.info(
                "   ✅ [Phase 1] Non-destructive recovery successful! Recovered \(recoveredCount) bundles"
              )
              localCount = Int(recoveredCount)
              desyncDetected = localCount != serverStats.available
              // Skip destructive recovery
            } else {
              logger.warning(
                "   ⚠️ [Phase 1] Non-destructive recovery found 0 bundles - proceeding to Phase 2")

              // PHASE 2: Fall back to destructive recovery if non-destructive failed
              let normalizedDID = normalizeUserDID(userDID)
              if let deviceManager = self.deviceManagers[normalizedDID] {
                do {
                  logger.warning("   ⚠️ ⚠️ ⚠️ [Phase 2] INITIATING DESTRUCTIVE RECOVERY ⚠️ ⚠️ ⚠️")
                  logger.warning("   This will delete server bundles and clear local storage")
                  try await deviceManager.recoverFromKeyPackageDesync(userDid: userDID)
                  localCount = 0
                } catch {
                  logger.error("   ❌ Destructive recovery FAILED: \(error.localizedDescription)")
                  logger.error(
                    "      ACTION REQUIRED: Manually call deviceManager.recoverFromKeyPackageDesync(userDid:)"
                  )
                }
              } else {
                logger.error(
                  "   ❌ Cannot auto-recover: deviceManager not configured for user \(normalizedDID)"
                )
                logger.error(
                  "      ACTION: Call deviceManager.recoverFromKeyPackageDesync(userDid:)")
              }
            }
          } catch {
            logger.error("   ❌ Non-destructive recovery failed: \(error.localizedDescription)")

            // PHASE 2: Fall back to destructive recovery on error
            let normalizedDID = normalizeUserDID(userDID)
            if let deviceManager = self.deviceManagers[normalizedDID] {
              do {
                logger.warning("   ⚠️ ⚠️ ⚠️ [Phase 2] INITIATING DESTRUCTIVE RECOVERY ⚠️ ⚠️ ⚠️")
                logger.warning("   This will delete server bundles and clear local storage")
                try await deviceManager.recoverFromKeyPackageDesync(userDid: userDID)
                localCount = 0
              } catch {
                logger.error("   ❌ Destructive recovery FAILED: \(error.localizedDescription)")
                logger.error(
                  "      ACTION REQUIRED: Manually call deviceManager.recoverFromKeyPackageDesync(userDid:)"
                )
              }
            } else {
              logger.error(
                "   ❌ Cannot auto-recover: deviceManager not configured for user \(normalizedDID)")
              logger.error("      ACTION: Call deviceManager.recoverFromKeyPackageDesync(userDid:)")
            }
          }
        }
      } else if localCount > 0 && serverStats.available == 0 {
        logger.error("   ⚠️ SERVER DESYNC: Local has \(localCount) bundles but server has 0")
        logger.error("   📋 Root Cause: Bundles created locally but never uploaded to server")
        logger.error("   🔧 Recovery Required: Upload local bundles to server")
        logger.error(
          "      ACTION: Automatically calling monitorAndReplenishBundles() to upload bundles")

        do {
          logger.info("📤 Auto-repairing: Uploading \(localCount) local bundles to server...")
          let uploadResult = try await monitorAndReplenishBundles(for: userDID)
          logger.info(
            "✅ Auto-repair successful! Uploaded bundles - available: \(uploadResult.available), uploaded: \(uploadResult.uploaded)"
          )
          serverStats = try await apiClient.getKeyPackageStats()
          desyncDetected = localCount != serverStats.available
        } catch {
          logger.error("❌ Auto-repair failed: \(error.localizedDescription)")
          logger.error("   Manual intervention required: Restart app or call reregisterDevice()")
        }
      } else if localCount > serverStats.available {
        let difference = localCount - serverStats.available
        logger.error("   ⚠️ BUNDLE MISMATCH: Local has \(difference) extra bundles")
        logger.error("   📋 Possible Causes:")
        logger.error("      - Server consumed bundles but local cache not updated")
        logger.error("   🔧 Attempting surgical cleanup of consumed bundles...")

        // Fetch consumed packages from server
        do {
          let status = try await apiClient.getKeyPackageStatus(limit: 100)
          logger.info("   📊 Server Status Details:")
          logger.info("      - Total uploaded: \(status.totalUploaded)")
          logger.info("      - Available: \(status.available)")
          logger.info("      - Consumed: \(status.consumed)")
          logger.info(
            "      - Difference matches consumed count: \(difference == status.consumed ? "YES ✅" : "NO ❌")"
          )

          if let consumed = status.consumedPackages, !consumed.isEmpty {
            logger.info("   📜 Found \(consumed.count) consumed bundles on server")

            // CRITICAL FIX: Only delete consumed bundles if the associated group exists locally
            // If the group doesn't exist, we may still need the bundle to process the Welcome message
            // This prevents the "NoMatchingKeyPackage" error during account switch recovery
            var safeToDeleteHashes: [Data] = []
            var preservedCount = 0

            for pkg in consumed {
              guard let hashData = Data(hexEncoded: pkg.keyPackageHash) else {
                continue
              }

              // Check if the group exists locally
              if let groupIdHex = pkg.usedInGroup,
                let groupIdData = Data(hexEncoded: groupIdHex)
              {
                let groupExistsLocally = groupExists(for: userDID, groupId: groupIdData)

                if groupExistsLocally {
                  // Group exists locally - safe to delete the consumed bundle
                  safeToDeleteHashes.append(hashData)
                } else {
                  // Group does NOT exist locally - we may need this bundle to process Welcome
                  logger.warning(
                    "   🛡️ Preserving consumed bundle \(pkg.keyPackageHash.prefix(16))... - group \(groupIdHex.prefix(16))... not found locally"
                  )
                  logger.warning(
                    "      This bundle may be needed to process a pending Welcome message")
                  preservedCount += 1
                }
              } else {
                // No group info - safe to delete (historical data)
                safeToDeleteHashes.append(hashData)
              }
            }

            if preservedCount > 0 {
              logger.info(
                "   🛡️ Preserved \(preservedCount) bundle(s) for potential Welcome processing")
            }

            if !safeToDeleteHashes.isEmpty {
              logger.info(
                "   🗑️ Deleting \(safeToDeleteHashes.count) consumed bundles from local storage...")

              do {
                let deletedCount = try await deleteKeyPackageBundles(
                  for: userDID,
                  hashRefs: safeToDeleteHashes
                )

                logger.info("   ✅ Deleted \(deletedCount) bundles - desync resolved!")

                // Re-check counts after deletion
                let newLocalCount = Int(
                  try await runFFIWithRecovery(for: userDID) { ctx in
                    try ctx.getKeyPackageBundleCount()
                  })
                let newServerStats = try await apiClient.getKeyPackageStats()
                logger.info(
                  "   📊 Updated counts: Local=\(newLocalCount), Server=\(newServerStats.available)")

                // Update desync flag (allow for preserved bundles)
                let expectedLocalCount = newServerStats.available + preservedCount
                desyncDetected =
                  newLocalCount != expectedLocalCount && newLocalCount != newServerStats.available

                if !desyncDetected || preservedCount > 0 {
                  logger.info(
                    "   🎉 Key package reconciliation successful! Local and server now in sync.")
                } else {
                  logger.warning(
                    "   ⚠️ Desync remains after cleanup: Local=\(newLocalCount), Server=\(newServerStats.available)"
                  )
                  logger.warning("      May need to call reregisterDevice() if issues persist")
                }
              } catch {
                logger.error(
                  "   ❌ Failed to delete consumed bundles: \(error.localizedDescription)")
                logger.error(
                  "      Fallback: Monitor for issues or call reregisterDevice() manually")
              }
            } else if preservedCount > 0 {
              logger.info(
                "   ✅ All consumed bundles preserved for Welcome processing - no deletion needed")
              // Desync is expected in this case - don't flag as error
              desyncDetected = false
            } else {
              logger.warning("   ⚠️ Could not parse consumed bundle hashes from server")
              logger.warning("      Manual intervention may be required if issues persist")
            }
          } else {
            logger.warning(
              "   ⚠️ No consumed packages reported by server, but local has extra bundles")
            logger.warning("      This may indicate a different type of desync")
            logger.warning("      Monitor for issues or call reregisterDevice() if problems occur")
          }
        } catch {
          logger.warning("   ⚠️ Could not fetch consumption info: \(error.localizedDescription)")
          logger.warning(
            "      Desync remains unresolved - monitor for NoMatchingKeyPackage errors")
        }
      } else {
        logger.error(
          "   ⚠️ LOCAL STORAGE DESYNC: Server has \(serverStats.available - localCount) extra bundles"
        )
        logger.error("   📋 Possible Causes:")
        logger.error("      - Deserialization bug dropped bundles from local storage")
        logger.error("      - Storage corrupted after bundles were uploaded")
        
        // CRITICAL FIX: Automatically sync hashes to remove orphaned server packages
        // Orphaned packages cause NoMatchingKeyPackage when others try to add us
        logger.info("   🔄 [AUTO-RECOVERY] Syncing key package hashes to remove orphaned server packages...")
        
        do {
          let syncResult = try await syncKeyPackageHashes(for: userDID)
          
          if syncResult.orphanedCount > 0 {
            logger.info("   ✅ [AUTO-RECOVERY] Deleted \(syncResult.deletedCount) orphaned packages from server")
            logger.info("      - Orphaned packages (on server, not local): \(syncResult.orphanedCount)")
            logger.info("      - Remaining available on server: \(syncResult.remainingAvailable)")
            
            // Check if we need to replenish after orphan cleanup
            if syncResult.remainingAvailable < 20 {
              logger.info("   📦 [AUTO-RECOVERY] Replenishing key packages after orphan cleanup...")
              do {
                let replenishResult = try await monitorAndReplenishBundles(for: userDID)
                logger.info("   ✅ [AUTO-RECOVERY] Replenished to \(replenishResult.available) packages")
                desyncDetected = false  // Recovery successful
              } catch {
                logger.error("   ⚠️ Replenishment failed: \(error.localizedDescription)")
              }
            } else {
              desyncDetected = false  // Orphans cleaned up, remaining are sufficient
            }
          } else {
            logger.warning("   ⚠️ No orphaned packages found - desync cause may be different")
            logger.warning("      Consider calling deviceManager.reregisterDevice(userDid:)")
          }
        } catch {
          logger.error("   ❌ Hash sync failed: \(error.localizedDescription)")
          logger.error("   🔧 Recovery Required: Re-register to regenerate local bundles")
          logger.error("      ACTION: Call deviceManager.reregisterDevice(userDid:)")
        }
      }

      // Log diagnostics for visibility
      logger.warning("   📊 Diagnostic Info:")
      logger.warning("      - User DID: \(userDID.prefix(30))...")
      logger.warning("      - Local bundle count: \(localCount)")
      logger.warning("      - Server available: \(serverStats.available)")
      logger.warning("      - Server threshold: \(serverStats.threshold)")

      do {
        let status = try await apiClient.getKeyPackageStatus(limit: 3)
        if let consumed = status.consumedPackages, !consumed.isEmpty {
          logger.debug("   📜 Recent bundle consumption (last \(consumed.count)):")
          for (index, pkg) in consumed.enumerated() {
            logger.debug(
              "      [\(index + 1)] Hash: \(pkg.keyPackageHash.prefix(16))... | Consumed: \(pkg.consumedAt.date)"
            )
          }
        } else {
          logger.debug("   📜 No recent bundle consumption recorded")
        }
      } catch {
        logger.warning("   ⚠️ Could not query consumption history: \(error.localizedDescription)")
      }
    } else {
      logger.info("✅ [Reconciliation] Key packages in sync:")
      logger.info("   - Local bundles: \(localCount)")
      logger.info("   - Server available: \(serverStats.available)")
      logger.info("   - No desync detected")
    }

    return (
      serverAvailable: serverStats.available, localBundles: localCount,
      desyncDetected: desyncDetected
    )
  }

  // MARK: - Key Package Hash Synchronization (NoMatchingKeyPackage Prevention)

  /// Synchronize key packages at the hash level to prevent NoMatchingKeyPackage errors
  ///
  /// This method solves the root cause of the NoMatchingKeyPackage bug:
  /// - When a device loses its local key packages (app reinstall, storage corruption, etc.)
  ///   the server still has those key packages and will serve them to other users
  /// - When someone tries to add this user to a group, they get an old key package
  /// - The user's device receives a Welcome encrypted to a public key it no longer has
  /// - Result: NoMatchingKeyPackage error and corrupted group state
  ///
  /// This method:
  /// 1. Gets the current device ID (REQUIRED - fails if not registered)
  /// 2. Gets all local key package hashes from the device
  /// 3. Sends them to the server via syncKeyPackages endpoint
  /// 4. Server compares against its available (unconsumed) key packages FOR THIS DEVICE ONLY
  /// 5. Server deletes any "orphaned" packages (on server but not in local storage)
  /// 6. Returns the count of deleted orphaned packages
  ///
  /// MULTI-DEVICE SUPPORT:
  /// The device ID is REQUIRED to ensure only THIS device's key packages are synced.
  /// This prevents Device A from accidentally deleting Device B's packages.
  /// Device ID comes from registerDevice and is persisted in UserDefaults.
  ///
  /// Should be called:
  /// - On app launch after device registration
  /// - After account switch
  /// - When recovering from any storage corruption
  ///
  /// - Parameter userDID: User DID to sync key packages for
  /// - Returns: Tuple of (orphanedCount, deletedCount, remainingAvailable)
  /// - Throws: MLSError.configurationError if device is not registered
  public func syncKeyPackageHashes(for userDID: String) async throws -> (
    orphanedCount: Int, deletedCount: Int, remainingAvailable: Int
  ) {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error("❌ [SyncKeyPackages] API client not configured for user \(normalizedDID)")
      throw MLSError.configurationError
    }

    logger.info("🔄 [SyncKeyPackages] START - user: \(userDID.prefix(20))...")

    // Step 0: Get device ID (REQUIRED for multi-device support)
    guard let deviceInfo = await getDeviceInfo(for: userDID) else {
      logger.error("❌ [SyncKeyPackages] Device not registered - cannot sync without device ID")
      logger.error("   Call ensureDeviceRegistered() first to register this device")
      throw MLSError.configurationError
    }
    let deviceId = deviceInfo.deviceId
    logger.info("📱 [SyncKeyPackages] Device ID: \(deviceId)")

    // Step 1: Get all local key package hashes
    let localHashes: [String]
    do {
      localHashes = try await getLocalKeyPackageHashes(for: userDID)
      logger.info("📍 [SyncKeyPackages] Found \(localHashes.count) local key packages")
      if localHashes.isEmpty {
        logger.warning(
          "⚠️ [SyncKeyPackages] No local key packages found - all server packages are orphaned!")
      }
    } catch {
      logger.error("❌ [SyncKeyPackages] Failed to get local hashes: \(error.localizedDescription)")
      throw error
    }

    // Step 2: Call server to sync and delete orphaned packages (device ID is required)
    let result:
      (
        serverHashes: [String], orphanedCount: Int, deletedCount: Int, orphanedHashes: [String],
        remainingAvailable: Int
      )
    do {
      result = try await apiClient.syncKeyPackages(localHashes: localHashes, deviceId: deviceId)
      logger.info("📊 [SyncKeyPackages] Server response:")
      logger.info("   - Device: \(deviceId)")
      logger.info("   - Orphaned packages detected: \(result.orphanedCount)")
      logger.info("   - Orphaned packages deleted: \(result.deletedCount)")
      logger.info("   - Remaining available on server: \(result.remainingAvailable)")
    } catch {
      logger.error("❌ [SyncKeyPackages] Server sync failed: \(error.localizedDescription)")
      throw error
    }

    // Step 3: Log results and warnings
    if result.orphanedCount > 0 {
      logger.warning(
        "🗑️ [SyncKeyPackages] Deleted \(result.deletedCount) ORPHANED key packages from server")
      logger.warning(
        "   These packages were on the server but the device no longer has the private keys")
      logger.warning("   Root cause: App reinstall, storage corruption, or cache clear")

      if result.orphanedCount > 5 {
        logger.warning("   Orphaned hashes (first 5):")
        for (i, hash) in result.orphanedHashes.prefix(5).enumerated() {
          logger.warning("      [\(i)] \(hash.prefix(16))...")
        }
        logger.warning("   ... and \(result.orphanedCount - 5) more")
      } else if !result.orphanedHashes.isEmpty {
        logger.warning("   Orphaned hashes:")
        for (i, hash) in result.orphanedHashes.enumerated() {
          logger.warning("      [\(i)] \(hash.prefix(16))...")
        }
      }
    } else {
      logger.info("✅ [SyncKeyPackages] No orphaned key packages found - all synced!")
    }

    // Step 4: Check if replenishment is needed
    if result.remainingAvailable < 20 {
      logger.warning(
        "⚠️ [SyncKeyPackages] Low key package inventory: \(result.remainingAvailable) remaining")
      logger.warning("   Consider calling monitorAndReplenishBundles() to upload more")
    }

    logger.info("✅ [SyncKeyPackages] COMPLETE")

    return (
      orphanedCount: result.orphanedCount,
      deletedCount: result.deletedCount,
      remainingAvailable: result.remainingAvailable
    )
  }
}

/// Adapter to expose Keychain access to Rust FFI
/// This allows the Rust layer to store sensitive keys in the system Keychain
/// while keeping bulk data in SQLite.
public class MLSKeychainAdapter: KeychainAccess {
  public func read(key: String) throws -> Data? {
    return try MLSKeychainManager.shared.retrieve(forKey: key)
  }

  public func write(key: String, value: Data) throws {
    try MLSKeychainManager.shared.store(value, forKey: key)
  }

  public func delete(key: String) throws {
    try MLSKeychainManager.shared.delete(forKey: key)
  }
}
