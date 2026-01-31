//
//  MLSMembershipChangeObserver.swift
//  Catbird
//
//  Observes and surfaces MLS group membership changes to the UI.
//  Provides non-blocking notifications/toasts for member joins and departures.
//

import CatbirdMLSCore
import Foundation
import GRDB
import OSLog

// MARK: - Membership Change Types

/// Individual membership change event
public struct MLSMembershipChange: Sendable, Identifiable {
    public enum ChangeType: String, Sendable {
        case userAdded        // New user (base DID) joined
        case deviceAdded      // Existing user added a new device
        case userRemoved      // User (all devices) removed
        case deviceRemoved    // Single device removed (user still in group)
        case updated
        case roleChanged
        
        // Legacy compatibility
        case added
        case removed
    }
    
    public let id: String
    public let type: ChangeType
    public let memberDID: String        // Full member DID (may include device fragment)
    public let baseDID: String          // Base user DID (without device fragment)
    public let deviceID: String?        // Device ID if present (fragment after #)
    public let memberDisplayName: String?
    public let actorDID: String?
    public let actorDisplayName: String?
    public let epoch: Int64
    public let timestamp: Date
    
    public init(
        id: String = UUID().uuidString,
        type: ChangeType,
        memberDID: String,
        baseDID: String? = nil,
        deviceID: String? = nil,
        memberDisplayName: String? = nil,
        actorDID: String? = nil,
        actorDisplayName: String? = nil,
        epoch: Int64,
        timestamp: Date = Date()
    ) {
        self.id = id
        self.type = type
        self.memberDID = memberDID
        // Parse base DID and device ID from member DID
        let parsed = Self.parseMemberDID(memberDID)
        self.baseDID = baseDID ?? parsed.baseDID
        self.deviceID = deviceID ?? parsed.deviceID
        self.memberDisplayName = memberDisplayName
        self.actorDID = actorDID
        self.actorDisplayName = actorDisplayName
        self.epoch = epoch
        self.timestamp = timestamp
    }
    
    /// Parse a member DID into base DID and optional device ID
    /// - Parameter memberDID: Full member DID (e.g., "did:plc:abc123#device-uuid")
    /// - Returns: Tuple of (baseDID, deviceID)
    private static func parseMemberDID(_ memberDID: String) -> (baseDID: String, deviceID: String?) {
        if let hashIndex = memberDID.firstIndex(of: "#") {
            let baseDID = String(memberDID[..<hashIndex])
            let deviceID = String(memberDID[memberDID.index(after: hashIndex)...])
            return (baseDID, deviceID.isEmpty ? nil : deviceID)
        }
        return (memberDID, nil)
    }
    
    /// Human-readable description of the change
    public var description: String {
        let member = memberDisplayName ?? shortenDID(baseDID)
        
        switch type {
        case .userAdded:
            if let actor = actorDisplayName ?? actorDID.map(shortenDID) {
                return "\(member) was added by \(actor)"
            }
            return "\(member) joined"
        case .deviceAdded:
            let deviceLabel = deviceID.map { "device \($0.prefix(8))..." } ?? "a new device"
            return "\(member) added \(deviceLabel)"
        case .userRemoved:
            if let actor = actorDisplayName ?? actorDID.map(shortenDID) {
                return "\(member) was removed by \(actor)"
            }
            return "\(member) left"
        case .deviceRemoved:
            let deviceLabel = deviceID.map { "device \($0.prefix(8))..." } ?? "a device"
            return "\(member) removed \(deviceLabel)"
        case .added: // Legacy
            if let actor = actorDisplayName ?? actorDID.map(shortenDID) {
                return "\(member) was added by \(actor)"
            }
            return "\(member) joined"
        case .removed: // Legacy
            if let actor = actorDisplayName ?? actorDID.map(shortenDID) {
                return "\(member) was removed by \(actor)"
            }
            return "\(member) left"
        case .updated:
            return "\(member) updated their device"
        case .roleChanged:
            return "\(member)'s role was changed"
        }
    }
    
    private func shortenDID(_ did: String) -> String {
        if did.hasPrefix("did:plc:") {
            return String(did.dropFirst(8).prefix(8)) + "..."
        }
        return String(did.prefix(16)) + "..."
    }
}

/// Batch of membership changes for a single epoch transition
public struct MLSMembershipChangeBatch: Sendable, Identifiable {
    public let id: String
    public let conversationID: String
    public let fromEpoch: Int64
    public let toEpoch: Int64
    public let changes: [MLSMembershipChange]
    public let timestamp: Date
    
    public var hasChanges: Bool { !changes.isEmpty }
    
    /// Summary suitable for notification display
    public var notificationSummary: String {
        let usersAdded = changes.filter { $0.type == .userAdded || $0.type == .added }
        let devicesAdded = changes.filter { $0.type == .deviceAdded }
        let usersRemoved = changes.filter { $0.type == .userRemoved || $0.type == .removed }
        let devicesRemoved = changes.filter { $0.type == .deviceRemoved }
        
        var parts: [String] = []
        
        // Users added
        if usersAdded.count == 1, let first = usersAdded.first {
            parts.append("\(first.memberDisplayName ?? "Someone") joined")
        } else if usersAdded.count > 1 {
            parts.append("\(usersAdded.count) people joined")
        }
        
        // Devices added (for existing users)
        if devicesAdded.count == 1, let first = devicesAdded.first {
            parts.append("\(first.memberDisplayName ?? "Someone") added a device")
        } else if devicesAdded.count > 1 {
            // Group by user
            let uniqueUsers = Set(devicesAdded.map { $0.baseDID })
            if uniqueUsers.count == 1, let first = devicesAdded.first {
                parts.append("\(first.memberDisplayName ?? "Someone") added \(devicesAdded.count) devices")
            } else {
                parts.append("\(devicesAdded.count) devices added")
            }
        }
        
        // Users removed
        if usersRemoved.count == 1, let first = usersRemoved.first {
            parts.append("\(first.memberDisplayName ?? "Someone") left")
        } else if usersRemoved.count > 1 {
            parts.append("\(usersRemoved.count) people left")
        }
        
        // Devices removed
        if devicesRemoved.count == 1, let first = devicesRemoved.first {
            parts.append("\(first.memberDisplayName ?? "Someone") removed a device")
        } else if devicesRemoved.count > 1 {
            parts.append("\(devicesRemoved.count) devices removed")
        }
        
        return parts.isEmpty ? "Membership updated" : parts.joined(separator: " • ")
    }
    
    public init(
        id: String = UUID().uuidString,
        conversationID: String,
        fromEpoch: Int64,
        toEpoch: Int64,
        changes: [MLSMembershipChange],
        timestamp: Date = Date()
    ) {
        self.id = id
        self.conversationID = conversationID
        self.fromEpoch = fromEpoch
        self.toEpoch = toEpoch
        self.changes = changes
        self.timestamp = timestamp
    }
}

// MARK: - Membership Change Observer

/// Observes MLS group membership changes and surfaces them to the UI
///
/// This actor:
/// - Detects membership changes by comparing roster snapshots
/// - Creates MLSMembershipChange events for each detected change
/// - Surfaces changes via non-blocking notifications/toasts
/// - Records changes in the audit log
public actor MLSMembershipChangeObserver {
    
    // MARK: - Properties
    
    private let database: MLSDatabase
    private let currentUserDID: String
    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "blue.catbird", category: "MLSMembershipChangeObserver")
    
    /// Callback for surfacing membership changes to the UI
    private var changeHandler: ((MLSMembershipChangeBatch) async -> Void)?
    
    /// Profile lookup function (DID -> display name)
    private var profileLookup: ((String) async -> String?)?
    
    // MARK: - Initialization
    
    public init(database: MLSDatabase, currentUserDID: String) {
        self.database = database
        self.currentUserDID = currentUserDID
        logger.info("👥 MLSMembershipChangeObserver initialized")
    }
    
    /// Configure the change handler for UI notifications
    public func setChangeHandler(_ handler: @escaping (MLSMembershipChangeBatch) async -> Void) {
        self.changeHandler = handler
    }
    
    /// Configure the profile lookup function
    public func setProfileLookup(_ lookup: @escaping (String) async -> String?) {
        self.profileLookup = lookup
    }
    
    // MARK: - Change Detection
    
    /// Extract base DID from a member DID (removes device fragment if present)
    private func extractBaseDID(_ memberDID: String) -> String {
        if let hashIndex = memberDID.firstIndex(of: "#") {
            return String(memberDID[..<hashIndex])
        }
        return memberDID
    }
    
    /// Detect membership changes between two epochs
    /// - Parameters:
    ///   - conversationID: The conversation to check
    ///   - oldEpoch: Previous epoch number
    ///   - newEpoch: Current epoch number
    ///   - newMembers: Current member list (from FFI)
    ///   - actorDID: DID of the actor who triggered the change (if known)
    /// - Returns: Batch of detected changes, or nil if no changes
    public func detectChanges(
        conversationID: String,
        oldEpoch: Int64,
        newEpoch: Int64,
        newMembers: [String],
        actorDID: String? = nil
    ) async throws -> MLSMembershipChangeBatch? {
        logger.debug("🔍 Detecting changes for \(conversationID.prefix(8))... epoch \(oldEpoch) → \(newEpoch)")
        
        // Fetch previous roster snapshot
        let previousSnapshot = try await fetchRosterSnapshot(for: conversationID, epoch: oldEpoch)
        let previousMembers = Set(previousSnapshot?.memberDIDs ?? [])
        let currentMembers = Set(newMembers)
        
        // Calculate diff at member level (full DID including device)
        let addedDIDs = currentMembers.subtracting(previousMembers)
        let removedDIDs = previousMembers.subtracting(currentMembers)
        
        // No changes
        if addedDIDs.isEmpty && removedDIDs.isEmpty {
            logger.debug("   No membership changes detected")
            return nil
        }
        
        // Build base DID sets for classification
        let previousBaseDIDs = Set(previousMembers.map { extractBaseDID($0) })
        let currentBaseDIDs = Set(currentMembers.map { extractBaseDID($0) })
        
        // Build change events with proper classification
        var changes: [MLSMembershipChange] = []
        
        for did in addedDIDs {
            let baseDID = extractBaseDID(did)
            let displayName = await profileLookup?(baseDID)
            
            // Classify: Is this a new user or a new device for existing user?
            let isNewUser = !previousBaseDIDs.contains(baseDID)
            let changeType: MLSMembershipChange.ChangeType = isNewUser ? .userAdded : .deviceAdded
            
            changes.append(MLSMembershipChange(
                type: changeType,
                memberDID: did,
                memberDisplayName: displayName,
                actorDID: actorDID,
                epoch: newEpoch
            ))
            
            logger.info("👥 \(isNewUser ? "User" : "Device") added: \(baseDID.prefix(20))...")
        }
        
        for did in removedDIDs {
            let baseDID = extractBaseDID(did)
            let displayName = await profileLookup?(baseDID)
            
            // Classify: Is this user completely removed or just one device?
            let isUserGone = !currentBaseDIDs.contains(baseDID)
            let changeType: MLSMembershipChange.ChangeType = isUserGone ? .userRemoved : .deviceRemoved
            
            changes.append(MLSMembershipChange(
                type: changeType,
                memberDID: did,
                memberDisplayName: displayName,
                actorDID: actorDID,
                epoch: newEpoch
            ))
            
            logger.info("👥 \(isUserGone ? "User" : "Device") removed: \(baseDID.prefix(20))...")
        }
        
        let batch = MLSMembershipChangeBatch(
            conversationID: conversationID,
            fromEpoch: oldEpoch,
            toEpoch: newEpoch,
            changes: changes
        )
        
        logger.info("👥 Detected \(changes.count) membership changes: \(batch.notificationSummary)")
        
        return batch
    }
    
    /// Process and surface membership changes after a commit merge
    /// - Parameters:
    ///   - conversationID: The conversation that changed
    ///   - oldEpoch: Previous epoch
    ///   - newEpoch: New epoch after commit
    ///   - newMembers: Current member list from FFI
    ///   - treeHash: Tree hash for the new epoch (for pinning)
    ///   - actorDID: DID of the commit author (if known)
    public func processEpochTransition(
        conversationID: String,
        oldEpoch: Int64,
        newEpoch: Int64,
        newMembers: [String],
        treeHash: Data?,
        actorDID: String? = nil
    ) async throws {
        // 1. Detect changes
        guard let batch = try await detectChanges(
            conversationID: conversationID,
            oldEpoch: oldEpoch,
            newEpoch: newEpoch,
            newMembers: newMembers,
            actorDID: actorDID
        ) else {
            // No membership changes, but still save roster snapshot
            try await saveRosterSnapshot(
                conversationID: conversationID,
                epoch: newEpoch,
                members: newMembers,
                treeHash: treeHash
            )
            return
        }
        
        // 2. Save roster snapshot
        try await saveRosterSnapshot(
            conversationID: conversationID,
            epoch: newEpoch,
            members: newMembers,
            treeHash: treeHash
        )
        
        // 3. Record membership events
        try await recordMembershipEvents(batch)
        
        // 4. Surface changes via UI callback
        if let handler = changeHandler {
            await handler(batch)
        }
    }
    
    // MARK: - Roster Snapshot Management
    
    private func fetchRosterSnapshot(for conversationID: String, epoch: Int64) async throws -> MLSRosterSnapshotModel? {
        try await database.read { db in
            try MLSRosterSnapshotModel
                .filter(Column("conversationID") == conversationID)
                .filter(Column("epoch") == epoch)
                .fetchOne(db)
        }
    }
    
    private func saveRosterSnapshot(
        conversationID: String,
        epoch: Int64,
        members: [String],
        treeHash: Data?
    ) async throws {
        // Get previous snapshot ID for chain
        let previousSnapshot = try await database.read { db in
            try MLSRosterSnapshotModel
                .filter(Column("conversationID") == conversationID)
                .order(Column("epoch").desc)
                .fetchOne(db)
        }
        
        let snapshot = MLSRosterSnapshotModel(
            conversationID: conversationID,
            epoch: epoch,
            memberDIDs: members,
            treeHash: treeHash,
            previousSnapshotID: previousSnapshot?.snapshotID
        )
        
        try await database.write { db in
            try snapshot.insert(db)
        }
        
        logger.debug("💾 Saved roster snapshot for epoch \(epoch) with \(members.count) members")
    }
    
    private func recordMembershipEvents(_ batch: MLSMembershipChangeBatch) async throws {
        try await database.write { db in
            for change in batch.changes {
                let eventType: MLSMembershipEventModel.EventType
                switch change.type {
                case .userAdded, .added: 
                    eventType = .joined
                case .deviceAdded:
                    eventType = .deviceAdded
                case .userRemoved, .removed:
                    eventType = .left
                case .deviceRemoved:
                    eventType = .deviceRemoved
                case .updated:
                    eventType = .deviceAdded
                case .roleChanged:
                    eventType = .roleChanged
                }
                
                let event = MLSMembershipEventModel(
                    id: change.id,
                    conversationID: batch.conversationID,
                    currentUserDID: currentUserDID,
                    memberDID: change.memberDID,
                    eventType: eventType,
                    timestamp: change.timestamp,
                    actorDID: change.actorDID,
                    epoch: change.epoch,
                    metadata: nil
                )
                
                try event.insert(db)
            }
        }
        
        logger.debug("📝 Recorded \(batch.changes.count) membership events")
    }
    
    // MARK: - History Queries
    
    /// Get recent membership changes for a conversation
    public func getRecentChanges(
        for conversationID: String,
        limit: Int = 50
    ) async throws -> [MLSMembershipEventModel] {
        try await database.read { db in
            try MLSMembershipEventModel
                .filter(Column("conversationID") == conversationID)
                .order(Column("timestamp").desc)
                .limit(limit)
                .fetchAll(db)
        }
    }
    
    /// Get roster at a specific epoch
    public func getRoster(
        for conversationID: String,
        at epoch: Int64
    ) async throws -> [String]? {
        let snapshot = try await fetchRosterSnapshot(for: conversationID, epoch: epoch)
        return snapshot?.memberDIDs
    }
}
