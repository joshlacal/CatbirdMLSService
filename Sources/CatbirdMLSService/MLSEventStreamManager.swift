import Foundation
import Petrel
import OSLog
import Combine

/// Manages SSE (Server-Sent Events) subscriptions for MLS conversations
/// Provides real-time message delivery and reactions
/// Actor isolation keeps long-running stream work off the main thread while
/// preserving thread-safe access to subscription state.
public actor MLSEventStreamManager {
    private let logger = Logger(subsystem: "blue.catbird", category: "MLSEventStream")
    
    // MARK: - Properties

    private let apiClient: MLSAPIClient
    private var activeSubscriptions: [String: Task<Void, Never>] = [:]
    private var eventHandlers: [String: EventHandler] = [:]

    private var connectionState: [String: ConnectionState] = [:]
    private var lastCursor: [String: String] = [:]
    
    /// Flags to signal graceful shutdown (not cancellation)
    /// This allows the SSE loop to exit cleanly without CancellationError
    private var shouldStop: [String: Bool] = [:]

    /// Optional persistent cursor storage (survives app restart)
    private var cursorStore: MLSEventCursorStore?
    
    // MARK: - Types
    
    public enum ConnectionState {
        case disconnected
        case connecting
        case connected
        case reconnecting
        case error(Error)
    }
    
    public struct EventHandler {
        public var onMessage: ((BlueCatbirdMlsSubscribeConvoEvents.MessageEvent) async -> Void)?
        public var onReaction: ((BlueCatbirdMlsSubscribeConvoEvents.ReactionEvent) async -> Void)?
        public var onInfo: ((BlueCatbirdMlsSubscribeConvoEvents.InfoEvent) async -> Void)?
        public var onNewDevice: ((BlueCatbirdMlsSubscribeConvoEvents.NewDeviceEvent) async -> Void)?
        public var onGroupInfoRefreshRequested: ((BlueCatbirdMlsSubscribeConvoEvents.GroupInfoRefreshRequestedEvent) async -> Void)?
        public var onReadditionRequested:
            ((BlueCatbirdMlsSubscribeConvoEvents.ReadditionRequestedEvent) async -> Void)?
        public var onMembershipChanged: ((String, DID, MembershipAction) async -> Void)?
        public var onKickedFromConversation: ((String, DID, String?) async -> Void)?
        public var onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)?
        public var onError: ((Error) async -> Void)?
        public var onReconnected: (() async -> Void)?
        
        public init() {}

        public init(
            onMessage: ((BlueCatbirdMlsSubscribeConvoEvents.MessageEvent) async -> Void)? = nil,
            onReaction: ((BlueCatbirdMlsSubscribeConvoEvents.ReactionEvent) async -> Void)? = nil,
            onInfo: ((BlueCatbirdMlsSubscribeConvoEvents.InfoEvent) async -> Void)? = nil,
            onNewDevice: ((BlueCatbirdMlsSubscribeConvoEvents.NewDeviceEvent) async -> Void)? = nil,
            onGroupInfoRefreshRequested: ((BlueCatbirdMlsSubscribeConvoEvents.GroupInfoRefreshRequestedEvent) async -> Void)? = nil,
            onReadditionRequested: ((BlueCatbirdMlsSubscribeConvoEvents.ReadditionRequestedEvent) async -> Void)? = nil,
            onMembershipChanged: ((String, DID, MembershipAction) async -> Void)? = nil,
            onKickedFromConversation: ((String, DID, String?) async -> Void)? = nil,
            onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)? = nil,
            onError: ((Error) async -> Void)? = nil,
            onReconnected: (() async -> Void)? = nil
        ) {
            self.onMessage = onMessage
            self.onReaction = onReaction
            self.onInfo = onInfo
            self.onNewDevice = onNewDevice
            self.onGroupInfoRefreshRequested = onGroupInfoRefreshRequested
            self.onReadditionRequested = onReadditionRequested
            self.onMembershipChanged = onMembershipChanged
            self.onKickedFromConversation = onKickedFromConversation
            self.onConversationNeedsRecovery = onConversationNeedsRecovery
            self.onError = onError
            self.onReconnected = onReconnected
        }
    }
    
    // MARK: - Initialization

    public init(apiClient: MLSAPIClient) {
        self.apiClient = apiClient
    }

    // MARK: - Configuration

    /// Configure persistent cursor storage for surviving app restarts
    /// - Parameter store: The CursorStore instance to use for persistence
    public func configureCursorStore(_ store: MLSEventCursorStore) {
        self.cursorStore = store
        logger.info("CursorStore configured for persistent cursor storage")
    }

    // MARK: - Public Methods
    
    /// Subscribe to real-time events for a conversation
    /// - Parameters:
    ///   - convoId: Conversation ID to subscribe to
    ///   - cursor: Optional cursor to resume from (for reconnection)
    ///   - handler: Event handler for different event types
    public func subscribe(
        to convoId: String,
        cursor: String? = nil,
        handler: EventHandler
    ) {
        print("[SSE] subscribe() called for convoId: \(convoId.prefix(12))...")
        logger.info("📡 SSE: subscribe() called for convoId: \(convoId), cursor: \(cursor ?? "nil")")

        // Stop existing subscription if any
        stop(convoId)

        // Store handler and reset stop flag
        eventHandlers[convoId] = handler
        shouldStop[convoId] = false
        logger.info("📡 SSE: Handler registered for convoId: \(convoId)")

        // Update state
        connectionState[convoId] = .connecting
        logger.info("📡 SSE: State set to .connecting for convoId: \(convoId)")

        // Determine effective cursor: provided > in-memory > persistent store
        let effectiveCursor = cursor ?? lastCursor[convoId]

        // Start subscription task as DETACHED to survive view lifecycle changes
        // The task checks shouldStop[convoId] flag for graceful shutdown
        // This prevents CancellationError from propagating to the SSE stream
        let task = Task.detached(priority: .utility) { [weak self] in
            guard let self = self else { return }
            // Try to load from persistent store if no cursor available
            var cursorToUse = effectiveCursor
            if cursorToUse == nil, let store = await self.cursorStore {
                cursorToUse = await self.loadPersistentCursor(for: convoId, store: store)
            }
            await self.runSubscription(convoId: convoId, cursor: cursorToUse)
        }

        activeSubscriptions[convoId] = task
    }

    /// Load cursor from persistent storage
    private func loadPersistentCursor(for convoId: String, store: MLSEventCursorStore) async -> String? {
        do {
            let cursor = try await MainActor.run {
                try store.getCursor(for: convoId)
            }
            if let cursor = cursor {
                logger.info("📍 Loaded persistent cursor for \(convoId): \(cursor.prefix(20))...")
            }
            return cursor
        } catch {
            logger.warning("⚠️ Failed to load persistent cursor for \(convoId): \(error.localizedDescription)")
            return nil
        }
    }
    
    /// Stop subscription for a specific conversation
    /// - Parameter convoId: Conversation ID
    public func stop(_ convoId: String) {
        logger.info("Stopping subscription for: \(convoId)")
        
        // Set the graceful shutdown flag FIRST so the loop can exit cleanly
        shouldStop[convoId] = true
        
        activeSubscriptions[convoId]?.cancel()
        activeSubscriptions.removeValue(forKey: convoId)
        eventHandlers.removeValue(forKey: convoId)
        connectionState[convoId] = .disconnected
    }
    
    /// Stop all active subscriptions (synchronous - for quick cancellation)
    public func stopAll() {
        logger.info("Stopping all subscriptions")
        
        for convoId in activeSubscriptions.keys {
            stop(convoId)
        }
    }
    
    /// Stop all subscriptions and wait for them to complete
    /// CRITICAL: Call this during account switching to ensure all SSE tasks have
    /// finished writing to the database before closing it
    /// - Parameter timeout: Maximum time to wait for tasks to complete (default 2 seconds)
    public func stopAllAndWait(timeout: TimeInterval = 2.0) async {
        logger.info("🛑 Stopping all subscriptions and waiting for completion...")
        
        // Capture tasks before stopping (stop() removes them from the dictionary)
        let tasksToWait = Array(activeSubscriptions.values)
        let convoIds = Array(activeSubscriptions.keys)
        
        // Set all stop flags first to signal graceful shutdown
        for convoId in convoIds {
            shouldStop[convoId] = true
        }
        
        // Cancel all tasks
        for convoId in convoIds {
            stop(convoId)
        }
        
        // Wait for all tasks to complete with timeout
        if !tasksToWait.isEmpty {
            logger.info("   Waiting for \(tasksToWait.count) SSE task(s) to complete...")
            
            await withTaskGroup(of: Void.self) { group in
                // Add task to wait for all SSE tasks
                group.addTask {
                    for task in tasksToWait {
                        // Wait for each task to complete (they're already cancelled)
                        _ = await task.result
                    }
                }
                
                // Add timeout task
                group.addTask {
                    try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                }
                
                // Wait for whichever finishes first
                _ = await group.next()
                group.cancelAll()
            }
            
            logger.info("✅ All SSE tasks stopped")
        } else {
            logger.info("✅ No active SSE tasks to wait for")
        }
    }
    
    /// Reconnect to a conversation (using last cursor)
    /// - Parameter convoId: Conversation ID
    public func reconnect(_ convoId: String) {
        guard let handler = eventHandlers[convoId] else {
            logger.warning("No handler found for reconnection: \(convoId)")
            return
        }
        
        logger.info("Reconnecting to conversation: \(convoId)")
        
        let cursor = lastCursor[convoId]
        subscribe(to: convoId, cursor: cursor, handler: handler)
    }
    
    // MARK: - Private Methods
    
    private func runSubscription(convoId: String, cursor: String?) async {
        print("[SSE] runSubscription() started for convoId: \(convoId.prefix(12))...")
        logger.info("📡 SSE: runSubscription() started for convoId: \(convoId), cursor: \(cursor ?? "nil")")
        var reconnectAttempts = 0
        let maxReconnectAttempts = 5
        let reconnectDelay: TimeInterval = 2.0

        // Check both Task.isCancelled and shouldStop flag for graceful shutdown
        while !Task.isCancelled && shouldStop[convoId] != true && reconnectAttempts < maxReconnectAttempts {
            let connectionStartTime = Date()
            
            do {
                // Connect to SSE event stream
                print("[SSE] Attempting connection for: \(convoId.prefix(12))..., attempt: \(reconnectAttempts + 1)")
                logger.info("📡 SSE: Attempting connection for: \(convoId), attempt: \(reconnectAttempts + 1)")

                connectionState[convoId] = .connecting

                // Get event stream from API client via SSE
                // Always use the latest in-memory cursor for reconnect attempts to avoid replaying
                // already-processed events (and missing events during transient disconnects).
                let cursorToUse = lastCursor[convoId] ?? cursor
                let eventStream = try await apiClient.subscribeConvoEvents(
                    convoId: convoId,
                    cursor: cursorToUse
                )

                connectionState[convoId] = .connected
                print("[SSE] Connected to: \(convoId.prefix(12))... - entering event loop")
                logger.info("📡 SSE: State set to .connected for convoId: \(convoId) - entering event loop")

                // If this is a successful reconnection (not initial connection), trigger catchup
                if reconnectAttempts > 0 {
                    logger.info("✅ Reconnected successfully for: \(convoId) after \(reconnectAttempts) attempts - triggering catchup")
                    if let handler = eventHandlers[convoId], let reconnectedHandler = handler.onReconnected {
                        await reconnectedHandler()
                    }
                }

                // Process events from stream
                print("[SSE] Starting event loop for: \(convoId.prefix(12))..., waiting for events...")
                logger.info("📡 SSE: Starting event loop for convoId: \(convoId)")
                var eventCount = 0
                for try await output in eventStream {
                    // Check for graceful shutdown signal
                    if shouldStop[convoId] == true {
                        logger.info("📡 SSE: Graceful shutdown requested for: \(convoId)")
                        break
                    }
                    
                    eventCount += 1
                    print("[SSE] 📡 Event #\(eventCount) received for: \(convoId.prefix(12))...")
                    logger.info("📡 SSE: Event #\(eventCount) received from stream for convoId: \(convoId)")
                    await handleEvent(output, for: convoId)
                }
                
                // Check if we're stopping gracefully
                if shouldStop[convoId] == true {
                    logger.info("📡 SSE: Exiting loop due to graceful shutdown for: \(convoId)")
                    break
                }
                
                print("[SSE] Stream ended for: \(convoId.prefix(12))..., received \(eventCount) events, duration: \(Date().timeIntervalSince(connectionStartTime))s")
                // Check if connection was stable for a while (reset retries if > 5 seconds)
                let duration = Date().timeIntervalSince(connectionStartTime)
                if duration > 5.0 {
                    reconnectAttempts = 0
                }

                // If we reach here, connection was closed
                if eventCount == 0 {
                    // Stream closed immediately without any events - treat as error and retry
                    logger.warning("📡 SSE: Stream closed with 0 events for: \(convoId) - will retry")
                    reconnectAttempts += 1
                } else {
                    logger.info("📡 SSE: Stream ended after \(eventCount) events for: \(convoId) - reconnecting")
                    // If connection was short but had events, treat as unstable
                    if duration < 5.0 {
                        reconnectAttempts += 1
                    }
                }
                
                if reconnectAttempts < maxReconnectAttempts && reconnectAttempts > 0 && shouldStop[convoId] != true {
                    connectionState[convoId] = .reconnecting
                    try? await Task.sleep(nanoseconds: UInt64(reconnectDelay * Double(reconnectAttempts) * 1_000_000_000))
                }

            } catch {
                // Check if this is a cancellation error during graceful shutdown
                if shouldStop[convoId] == true || Task.isCancelled {
                    logger.info("📡 SSE: Exiting due to shutdown/cancellation for: \(convoId)")
                    break
                }
                
                print("[SSE] Connection error for \(convoId.prefix(12))...: \(error.localizedDescription)")
                logger.error("📡 SSE: Connection error for \(convoId): \(error.localizedDescription) - \(String(describing: error))")

                connectionState[convoId] = .error(error)

                // Notify error handler
                if let handler = eventHandlers[convoId], let errorHandler = handler.onError {
                    await errorHandler(error)
                }
                
                // Check duration for reset
                if Date().timeIntervalSince(connectionStartTime) > 5.0 {
                    reconnectAttempts = 0
                }

                // Attempt reconnect only if not shutting down
                if !Task.isCancelled && shouldStop[convoId] != true {
                    reconnectAttempts += 1

                    if reconnectAttempts < maxReconnectAttempts {
                        logger.info("Attempting reconnect \(reconnectAttempts)/\(maxReconnectAttempts) for: \(convoId)")
                        connectionState[convoId] = .reconnecting

                        try? await Task.sleep(nanoseconds: UInt64(reconnectDelay * Double(reconnectAttempts) * 1_000_000_000))
                    }
                }
            }
        }

        if reconnectAttempts >= maxReconnectAttempts {
            logger.error("Max reconnect attempts reached for: \(convoId)")
            connectionState[convoId] = .disconnected
        } else if shouldStop[convoId] == true {
            logger.info("📡 SSE: Subscription stopped gracefully for: \(convoId)")
            connectionState[convoId] = .disconnected
        }
    }
    
    private func handleEvent(_ message: BlueCatbirdMlsSubscribeConvoEvents.Message, for convoId: String) async {
        guard let handler = eventHandlers[convoId] else {
            logger.warning("📡 SSE: No handler found for convoId: \(convoId) - event dropped!")
            return
        }

        logger.info("📡 SSE: handleEvent() called for convoId: \(convoId)")

        // Handle the event based on the union type
        switch message {
        case .messageEvent(let messageEvent):
            print("[SSE] 📨 MESSAGE EVENT received - id: \(messageEvent.message.id.prefix(12))...")
            logger.info("📡 SSE: MESSAGE EVENT received - id: \(messageEvent.message.id), calling onMessage handler")
            saveCursor(messageEvent.cursor, for: convoId)
            await handler.onMessage?(messageEvent)

        case .reactionEvent(let reactionEvent):
            logger.info(
                "📡 SSE: REACTION EVENT received - convo: \(convoId.prefix(16)), action: \(reactionEvent.action), reaction: \(reactionEvent.reaction), calling onReaction handler")
            saveCursor(reactionEvent.cursor, for: convoId)
            await handler.onReaction?(reactionEvent)

        case .typingEvent(let typingEvent):
            // Typing indicators removed - ignore event
            saveCursor(typingEvent.cursor, for: convoId)

        case .infoEvent(let infoEvent):
            logger.info(
                "📡 SSE: INFO EVENT received - convo: \(convoId.prefix(16)), info: \(infoEvent.info), calling onInfo handler")
            saveCursor(infoEvent.cursor, for: convoId)
            await handler.onInfo?(infoEvent)

        case .newDeviceEvent(let newDeviceEvent):
            logger.info("New device event: user=\(newDeviceEvent.userDid), device=\(newDeviceEvent.deviceId), convo=\(newDeviceEvent.convoId)")
            saveCursor(newDeviceEvent.cursor, for: convoId)
            await handler.onNewDevice?(newDeviceEvent)

        case .groupInfoRefreshRequestedEvent(let refreshEvent):
            logger.info(
                "📡 SSE: GROUP INFO REFRESH REQ received - convo: \(refreshEvent.convoId.prefix(16)), requestedBy: \(refreshEvent.requestedBy)")
            saveCursor(refreshEvent.cursor, for: convoId)
            await handler.onGroupInfoRefreshRequested?(refreshEvent)

        case .readditionRequestedEvent(let readditionEvent):
            logger.info("Re-addition requested: convo=\(readditionEvent.convoId), user=\(readditionEvent.userDid)")
            saveCursor(readditionEvent.cursor, for: convoId)
            await handler.onReadditionRequested?(readditionEvent)

        // MARK: - Membership Change Events
        case .membershipChangeEvent(let membershipEvent):
            logger.info("Membership change: convo=\(membershipEvent.convoId), did=\(membershipEvent.did), action=\(membershipEvent.action)")
            saveCursor(membershipEvent.cursor, for: convoId)
            if let action = MembershipAction(rawValue: membershipEvent.action) {
                await handler.onMembershipChanged?(membershipEvent.convoId, membershipEvent.did, action)
            }

            // If the current user was kicked/removed, notify via special handler
            // Note: We need access to current user's DID to determine this
            // This will be handled by the view layer that has access to the current user

        // MARK: - Read Receipt Events (Removed)
        case .readEvent(let readEvent):
            // Read receipts removed - ignore event
            saveCursor(readEvent.cursor, for: convoId)
        }
    }
    
    /// Save cursor to both in-memory cache and persistent storage
    private func saveCursor(_ cursor: String, for convoId: String) {
        lastCursor[convoId] = cursor
        
        // Persist asynchronously to avoid blocking event processing
        if let store = cursorStore {
            Task {
                do {
                    try await MainActor.run {
                        try store.updateCursor(for: convoId, cursor: cursor)
                    }
                } catch {
                    logger.warning("⚠️ Failed to persist cursor for \(convoId): \(error.localizedDescription)")
                }
            }
        }
    }
}

// NOTE: SSE event stream implementation is now in MLSAPIClient.swift
