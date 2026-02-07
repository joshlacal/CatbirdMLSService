
import Foundation
import OSLog
import Petrel

public extension MLSConversationManager {

  // MARK: - Key Package Management

  /// Publish a new key package for the current user
  /// - Parameter expiresAt: Optional expiration date (defaults to 30 days)
  /// - Returns: Published key package reference
  @discardableResult
  public func publishKeyPackage(expiresAt: Date? = nil) async throws -> BlueCatbirdMlsDefs.KeyPackageRef {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    return try await keyPackageManager.publishKeyPackage(for: userDid, expiresAt: expiresAt)
  }

  /// Smart key package refresh using monitor (preferred method)
  public func smartRefreshKeyPackages(maxGeneratedPackages: Int? = nil) async throws {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    try await keyPackageManager.smartRefreshKeyPackages(
      for: userDid,
      isShuttingDown: isShuttingDown,
      maxGeneratedPackages: maxGeneratedPackages
    )
  }

  /// Basic refresh without smart monitoring (fallback/legacy)
  public func refreshKeyPackagesBasic() async throws {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    try await keyPackageManager.refreshKeyPackagesBasic(for: userDid)
  }

  /// Legacy method for backward compatibility
  public func refreshKeyPackagesIfNeeded() async throws {
    try await smartRefreshKeyPackages()
  }

  /// Smart batch upload using batch API (preferred method)
  /// Ensures local key packages exist AND uploads to server if needed
  public func uploadKeyPackageBatchSmart(
    count: Int = 100,
    maxGeneratedPackages: Int? = nil
  ) async throws {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }
    try await keyPackageManager.uploadKeyPackageBatchSmart(
      for: userDid,
      count: count,
      maxGeneratedPackages: maxGeneratedPackages
    )
  }

  /// Legacy batch upload method for backward compatibility
  public func uploadKeyPackageBatch(count: Int = 100) async throws {
    try await uploadKeyPackageBatchSmart(count: count)
  }

  /// Refresh key packages based on time interval
  public func refreshKeyPackagesBasedOnInterval(maxGeneratedPackages: Int? = nil) async throws {
    guard let userDid = userDid else {
      // If no userDid, we can't refresh. Just return.
      return
    }
    try await keyPackageManager.refreshKeyPackagesBasedOnInterval(
      for: userDid,
      isShuttingDown: isShuttingDown,
      maxGeneratedPackages: maxGeneratedPackages
    )
  }

}
