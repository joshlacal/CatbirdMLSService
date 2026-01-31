import Observation
import Synchronization

/// Thread-safe dictionary wrapper for use with @Observable classes.
///
/// This type provides concurrent-safe dictionary access using `Mutex<[Key: Value]>` for storage,
/// while supporting SwiftUI observation via the `@Observable` macro. It solves the threading crash
/// when multiple async tasks read/write dictionaries from an @Observable class.
///
/// The crash pattern was: "unrecognized selector sent to instance" during Dictionary lookup
/// caused by Swift's COW semantics and ObjC dynamic dispatch accessing freed memory when
/// concurrent threads mutate a `var` dictionary property.
///
/// Usage:
/// ```swift
/// @Observable
/// public final class MyManager {
///   public let conversations = ObservableMutexDictionary<String, Conversation>()
///
///   func update() {
///     conversations["id"] = newValue  // Thread-safe subscript
///   }
/// }
/// ```
@Observable
public final class ObservableMutexDictionary<Key: Hashable, Value>: @unchecked Sendable {
  @ObservationIgnored
  private let storage = Mutex<[Key: Value]>([:])

  public init(_ initialValue: [Key: Value] = [:]) {
    storage.withLock { $0 = initialValue }
  }

  public subscript(key: Key) -> Value? {
    get { storage.withLock { $0[key] } }
    set { withMutation(keyPath: \.self) { storage.withLock { $0[key] = newValue } } }
  }

  public var keys: [Key] { storage.withLock { Array($0.keys) } }
  public var values: [Value] { storage.withLock { Array($0.values) } }
  public var count: Int { storage.withLock { $0.count } }
  public var isEmpty: Bool { storage.withLock { $0.isEmpty } }

  @discardableResult
  public func removeValue(forKey key: Key) -> Value? {
    withMutation(keyPath: \.self) { storage.withLock { $0.removeValue(forKey: key) } }
  }

  public func removeAll() {
    withMutation(keyPath: \.self) { storage.withLock { $0.removeAll() } }
  }

  /// Get a consistent snapshot of the entire dictionary for atomic operations.
  public func snapshot() -> [Key: Value] {
    storage.withLock { $0 }
  }
}

// MARK: - Sequence Conformance

extension ObservableMutexDictionary: Sequence {
  public typealias Element = (key: Key, value: Value)
  public typealias Iterator = Array<Element>.Iterator

  /// Creates an iterator over the snapshot of the dictionary.
  /// Note: This takes a snapshot at iteration start for thread safety.
  public func makeIterator() -> Iterator {
    storage.withLock { Array($0) }.makeIterator()
  }
}
