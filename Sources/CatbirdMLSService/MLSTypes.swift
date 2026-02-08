import CatbirdMLSCore
import Foundation
import Petrel

/// Errors specific to MLS conversation operations
public enum MLSConversationError: Error, LocalizedError {
  case invalidKeyPackage(String)
  case noAuthentication
  case contextNotInitialized
  case conversationNotFound
  case groupStateNotFound
  case groupNotInitialized
  case invalidWelcomeMessage
  case invalidIdentity
  case invalidGroupId
  case invalidMessage
  case invalidCiphertext
  case decodingFailed
  case decryptionFailed
  case invalidEpoch(String)
  case epochMismatch
  case missingKeyPackages([String])
  case operationFailed(String)
  case mlsError(String)
  case serverError(Error)
  case syncFailed(Error)
  case storageUnavailable(reason: String)
  case commitProcessingFailed(Int, Error)
  case memberSyncFailed
  case conversationNotReady
  case duplicateSend
  case invalidCredential
  case keyPackageDesyncRecoveryInitiated
  case duplicateMessage
  case welcomeFetchFailed

  public var errorDescription: String? {
    switch self {
    case .invalidKeyPackage(let message):
      return "Invalid key package format: \(message)"
    case .noAuthentication:
      return "User authentication required"
    case .contextNotInitialized:
      return "MLS context not initialized"
    case .conversationNotFound:
      return "Conversation not found"
    case .groupStateNotFound:
      return "Group state not found"
    case .groupNotInitialized:
      return "MLS group not initialized locally"
    case .invalidWelcomeMessage:
      return "Invalid Welcome message format"
    case .invalidIdentity:
      return "Invalid user identity"
    case .invalidGroupId:
      return "Invalid group identifier"
    case .invalidMessage:
      return "Invalid message format"
    case .invalidCiphertext:
      return "Invalid ciphertext format"
    case .decodingFailed:
      return "Failed to decode message"
    case .decryptionFailed:
      return "Message decryption failed"
    case .invalidEpoch(let message):
      return "Epoch mismatch: \(message)"
    case .epochMismatch:
      return "Local epoch doesn't match server epoch after commit merge"
    case .missingKeyPackages(let dids):
      return "Missing key packages for members: \(dids.joined(separator: ", "))"
    case .operationFailed(let message):
      return "Operation failed: \(message)"
    case .mlsError(let message):
      return "MLS protocol error: \(message)"
    case .serverError(let error):
      return "Server error: \(error.localizedDescription)"
    case .syncFailed(let error):
      return "Synchronization failed: \(error.localizedDescription)"
    case .storageUnavailable(let reason):
      return "Storage unavailable: \(reason)"
    case .commitProcessingFailed(let epoch, let error):
      return "Commit processing failed at epoch \(epoch): \(error.localizedDescription)"
    case .memberSyncFailed:
      return "Member synchronization with server failed"
    case .conversationNotReady:
      return "Conversation is not ready for messaging"
    case .duplicateSend:
      return "Duplicate message send detected"
    case .invalidCredential:
      return "Invalid MLS credential data"
    case .keyPackageDesyncRecoveryInitiated:
      return
        "Key package synchronization recovery initiated. Please rejoin the conversation when prompted."
    case .duplicateMessage:
      return "Duplicate message detected"
    case .welcomeFetchFailed:
      return "Failed to fetch Welcome message for group join"
    }
  }
}

/// MLS group state tracking
public struct MLSGroupState {
  public var groupId: String
  public var convoId: String
  public var epoch: UInt64
  public var members: Set<String>
  /// Last epoch reported by the server (from getConvos). Used to gate GroupInfo uploads.
  public var knownServerEpoch: UInt64?
}

/// Key package with hash tracking for lifecycle management
public struct KeyPackageWithHash {
  public let data: Data
  public let hash: String
  public let did: DID
}

/// Prepared data for sending Welcome/commit to server
public struct PreparedInitialMembers {
  public let commitData: Data
  public let welcomeData: Data
  public let hashEntries: [BlueCatbirdMlsCreateConvo.KeyPackageHashEntry]
  public let selectedPackages: [KeyPackageWithHash]  // Track for rollback on failure
}

/// Result returned after successfully creating a conversation on the server
public struct ServerConversationCreationResult {
  public let convo: BlueCatbirdMlsDefs.ConvoView
  public let commitData: Data?
  public let welcomeData: Data?
}

/// Pending operation
public struct MLSOperation {
  public enum OperationType {
    case addMembers([String])
    case sendMessage(String)
    case sync
  }

  public let id: UUID
  public let type: OperationType
  public let convoId: String?
  public var retryCount: Int
  public let createdAt: Date
}

/// Pending sent message tracking for proactive own-message identification
/// This prevents re-processing own sent messages through FFI, which would incorrectly advance the ratchet
public struct PendingMessage: Sendable {
  public let messageID: String  // Server-assigned message ID
  public let conversationID: String  // Conversation this message belongs to
  public let plaintext: String  // Original plaintext content
  public let embed: MLSEmbedData?  // Optional embed data (record, link, or GIF)
  public let senderDID: String  // Sender's DID (always currentUserDID for pending messages)
  public let timestamp: Date  // When the message was sent
  public let epoch: Int64  // MLS epoch when message was encrypted
  public let seq: Int64  // Server-assigned sequence number
}

/// State for message reorder buffer per conversation
/// Buffers out-of-order messages until their predecessors arrive
public struct MessageReorderState {
  /// Last successfully processed sequence number for this conversation
  public var lastProcessedSeq: Int64
  
  /// Messages waiting for predecessors to arrive (sorted by seq)
  public var bufferedMessages: [BufferedMessage]
  
  /// Timestamp when the buffer started waiting (for timeout)
  public var bufferStartTime: Date?
  
  public init(lastProcessedSeq: Int64 = -1) {
    self.lastProcessedSeq = lastProcessedSeq
    self.bufferedMessages = []
    self.bufferStartTime = nil
  }
  
  /// Check if we're currently waiting for messages
  public var isWaiting: Bool {
    !bufferedMessages.isEmpty
  }
  
  /// Get the next expected sequence number
  public var nextExpectedSeq: Int64 {
    lastProcessedSeq + 1
  }
}

/// A message waiting in the reorder buffer
public struct BufferedMessage {
  public let message: BlueCatbirdMlsDefs.MessageView
  public let receivedAt: Date
  
  public var seq: Int64 { Int64(message.seq) }
}

/// Membership action types
public enum MembershipAction: String, Codable, Sendable {
  case joined
  case left
  case removed
  case kicked
}

/// Membership change reason types
public enum MembershipChangeReason: Sendable, CustomStringConvertible {
  case selfLeft
  case kicked(by: DID, reason: String?)
  case outOfSync
  case connectionLost

  public var description: String {
    switch self {
    case .selfLeft:
      return "Self left the group"

    case .kicked(let by, let reason):
      if let reason {
        return "Kicked by \(by) (\(reason))"
      } else {
        return "Kicked by \(by)"
      }

    case .outOfSync:
      return "Local MLS state out of sync"

    case .connectionLost:
      return "Disconnected"
    }
  }
}

public enum RecoveryReason: String, Codable, Sendable {
  case epochMismatch
  case keyPackageDesync
  case memberRemoval
  case serverStateInconsistent
}

/// Outcome of processing a single MLS message
public enum MessageProcessingOutcome {
  case application(payload: MLSMessagePayload, sender: String)
  case nonApplication
  case controlMessage
}

/// Result of message processing with recovery
public enum MessageProcessingResult {
  case success(MessageProcessingOutcome)
  case skipped
  case failure(Error)
}
