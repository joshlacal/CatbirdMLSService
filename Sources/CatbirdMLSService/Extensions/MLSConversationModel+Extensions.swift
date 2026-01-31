import CatbirdMLSCore
import Foundation
import Petrel

public extension MLSConversationModel {
  /// Convert local database model to API ConvoView
  /// Note: Some fields are approximated since the local model doesn't store all API fields
  public func asConvoView() -> BlueCatbirdMlsDefs.ConvoView? {
    // Create DID from stored currentUserDID string
    guard let creatorDID = try? DID(didString: currentUserDID) else {
      return nil
    }

    return BlueCatbirdMlsDefs.ConvoView(
      groupId: conversationID,
      creator: creatorDID,
      members: [],  // Members not stored in local model, fetched separately when needed
      epoch: Int(epoch),
      cipherSuite: "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519",
      createdAt: ATProtocolDate(date: createdAt),
      lastMessageAt: lastMessageAt.map { ATProtocolDate(date: $0) },
      metadata: nil
    )
  }
}
