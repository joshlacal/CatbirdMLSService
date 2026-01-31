import Foundation

@MainActor
public protocol MLSEventCursorStore: AnyObject {
  func getCursor(for conversationId: String, eventType: String) throws -> String?
  func updateCursor(for conversationId: String, cursor: String, eventType: String) throws
}

public extension MLSEventCursorStore {
  func getCursor(for conversationId: String, eventType: String = "messageEvent") throws -> String? {
    try getCursor(for: conversationId, eventType: eventType)
  }

  func updateCursor(
    for conversationId: String,
    cursor: String,
    eventType: String = "messageEvent"
  ) throws {
    try updateCursor(for: conversationId, cursor: cursor, eventType: eventType)
  }
}
