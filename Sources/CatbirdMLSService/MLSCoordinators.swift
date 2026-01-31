import Foundation

// MARK: - Conversation Processing Coordinator

public actor ConversationProcessingCoordinator {
  private var lockedConversations: Set<String> = []
  private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]
  private var queueCounters: [String: Int64] = [:]

  public func withCriticalSection<T>(conversationID: String, operation: () async throws -> T)
    async rethrows -> T
  {
    await acquire(conversationID: conversationID)
    defer { release(conversationID: conversationID) }
    return try await operation()
  }

  public func withQueuedSection<T>(
    conversationID: String,
    operation: (Int64) async throws -> T
  ) async rethrows -> (queueIndex: Int64, result: T) {
    let queueIndex = (queueCounters[conversationID] ?? 0) + 1
    queueCounters[conversationID] = queueIndex

    await acquire(conversationID: conversationID)
    defer { release(conversationID: conversationID) }
    let result = try await operation(queueIndex)
    return (queueIndex, result)
  }

  private func acquire(conversationID: String) async {
    if !lockedConversations.contains(conversationID) {
      lockedConversations.insert(conversationID)
      return
    }

    await withCheckedContinuation { continuation in
      waiters[conversationID, default: []].append(continuation)
    }
  }

  private func release(conversationID: String) {
    if var queue = waiters[conversationID], !queue.isEmpty {
      let continuation = queue.removeFirst()
      waiters[conversationID] = queue
      continuation.resume()
    } else {
      lockedConversations.remove(conversationID)
      waiters[conversationID] = nil
    }
  }
}

// MARK: - Group Operation Coordinator

/// Actor-based coordinator for serializing MLS operations on the same group
/// Prevents concurrent mutations that could lose updates or cause ratchet desyncs
public actor GroupOperationCoordinator {
  private var activeOperations: [String: Task<Void, Never>] = [:]

  /// Execute an operation with exclusive access to a specific group
  /// Operations on the same group are serialized, but operations on different groups can run concurrently
  public func withExclusiveLock<T>(
    groupId: String,
    operation: @Sendable () async throws -> T
  ) async rethrows -> T {
    // Wait for any existing operation on this group
    while let existing = activeOperations[groupId] {
      _ = await existing.result
    }

    // Create a signal for when THIS operation completes
    let (stream, continuation) = AsyncStream<Void>.makeStream()

    // Store a task that waits for our signal
    // Future callers will wait on this task
    let trackingTask = Task<Void, Never> {
      for await _ in stream { break }
    }
    activeOperations[groupId] = trackingTask

    // Execute the actual operation
    defer {
      // Signal completion
      continuation.yield()
      continuation.finish()

      // Remove this group's lock if it's still our task
      // (It should be, since we're in an actor)
      if activeOperations[groupId] == trackingTask {
        activeOperations[groupId] = nil
      }
    }

    return try await operation()
  }

  /// PHASE 3 FIX: Execute a critical MLS operation that MUST complete atomically
  /// even if the parent task is cancelled. This protects operations that mutate
  /// MLS state (encrypt, decrypt, merge commit) from being interrupted mid-flight.
  ///
  /// Use this for operations that:
  /// - Consume one-time secrets from the MLS ratchet
  /// - Advance the epoch or sequence number
  /// - Must complete their side effects (e.g., database writes) atomically
  ///
  /// The operation runs in a detached task to prevent parent cancellation,
  /// while still maintaining exclusive group access via the coordinator.
  public func withUninterruptibleOperation<T>(
    groupId: String,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    return try await withExclusiveLock(groupId: groupId) {
      // Run in detached task to protect from parent cancellation
      // This ensures the operation completes even if the caller is cancelled
      try await Task.detached {
        try await operation()
      }.value
    }
  }
}

// MARK: - Send Queue Coordinator

/// Actor-based coordinator for serializing message sends per conversation
///
/// **Problem**: When the user rapidly sends "hello", "hi", "hey", these three sends
/// may complete their HTTP requests in any order depending on network timing,
/// causing messages to arrive at recipients out of order.
///
/// **Solution**: This coordinator ensures that each send waits for the previous
/// send to complete (receive server acknowledgment) before the next send starts.
/// This guarantees that server-assigned sequence numbers match send order.
///
/// Note: This is separate from `GroupOperationCoordinator` which handles
/// MLS cryptographic operations. This coordinator only serializes the
/// network submission portion to ensure ordering.
public actor SendQueueCoordinator {
  /// Per-conversation tracking of the last send task
  /// When a new send starts, it awaits the previous task's completion
  private var lastSendTask: [String: Task<Void, any Error>] = [:]
  
  /// Queue send depth counter for diagnostics
  private var queueDepth: [String: Int] = [:]
  
  /// Execute a send operation in strict FIFO order per conversation
  ///
  /// Each send waits for the previous send to complete before starting.
  /// This ensures that server-assigned sequence numbers match client send order.
  ///
  /// - Parameters:
  ///   - conversationID: The conversation to send to
  ///   - operation: The send operation (encrypt + network + cache)
  /// - Returns: The result of the send operation
  /// - Throws: If the send operation fails
  public func enqueueSend<T: Sendable>(
    conversationID: String,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    // Track queue depth for diagnostics
    let depth = (queueDepth[conversationID] ?? 0) + 1
    queueDepth[conversationID] = depth
    
    // Wait for any previous send to complete (non-throwing wait)
    if let previousTask = lastSendTask[conversationID] {
      // We wait for completion but ignore any error from the previous send
      // (each send should succeed/fail independently)
      _ = try? await previousTask.value
    }
    
    // Capture a completion signal for THIS send
    let (stream, continuation) = AsyncStream<Void>.makeStream()
    
    // Create a task that represents this send's completion (for next waiters)
    // This task completes when we signal it via the stream
    let completionTask = Task<Void, any Error> {
      for await _ in stream { break }
    }
    lastSendTask[conversationID] = completionTask
    
    // Ensure we signal completion even if the operation throws
    defer {
      continuation.yield()
      continuation.finish()
      
      // Decrement queue depth
      if let current = queueDepth[conversationID] {
        queueDepth[conversationID] = max(0, current - 1)
      }
      
      // Clean up if this was the last task
      if lastSendTask[conversationID] == completionTask {
        lastSendTask[conversationID] = nil
      }
    }
    
    // Execute the actual send operation
    return try await operation()
  }
  
  /// Get the current queue depth for a conversation (for diagnostics)
  public func getQueueDepth(conversationID: String) -> Int {
    return queueDepth[conversationID] ?? 0
  }
  
  /// Clear the queue for a conversation (e.g., on shutdown)
  public func clearQueue(conversationID: String) {
    lastSendTask.removeValue(forKey: conversationID)
    queueDepth.removeValue(forKey: conversationID)
  }
  
  /// Clear all queues (e.g., on account switch)
  public func clearAllQueues() {
    lastSendTask.removeAll()
    queueDepth.removeAll()
  }
}

