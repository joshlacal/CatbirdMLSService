
import Foundation
import OSLog
import Petrel

// This file previously contained typing indicator and read receipt event handling.
// These features have been removed to reduce complexity.
// 
// Reaction handling is performed directly in MLSConversationManager+Messaging.swift
// via notifyObservers(.reactionReceived(...))

public extension MLSConversationManager {
  // Event handling methods for typing indicators and read receipts have been removed.
  // SSE events for these types are no longer processed.
}
