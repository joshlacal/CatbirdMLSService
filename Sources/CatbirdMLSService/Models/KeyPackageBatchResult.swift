//
//  KeyPackageBatchResult.swift
//  Catbird
//
//  Created by Claude Code
//

import Foundation

/// Result from batch key package upload operation
public struct KeyPackageBatchResult: Codable, Sendable {
  /// Number of packages successfully uploaded
  public let succeeded: Int

  /// Number of packages that failed to upload
  public let failed: Int

  /// Individual error details (if any)
  public let errors: [BatchUploadError]?

  public init(succeeded: Int, failed: Int, errors: [BatchUploadError]? = nil) {
    self.succeeded = succeeded
    self.failed = failed
    self.errors = errors
  }

  /// Total number of packages in the batch
  public var total: Int {
    succeeded + failed
  }

  /// Whether the batch was completely successful
  public var isFullSuccess: Bool {
    failed == 0
  }

  /// Whether the batch was completely failed
  public var isFullFailure: Bool {
    succeeded == 0
  }

  /// Success rate as percentage
  public var successRate: Double {
    guard total > 0 else { return 0.0 }
    return Double(succeeded) / Double(total)
  }
}

/// Error details for individual package upload in batch
public struct BatchUploadError: Codable, Sendable {
  /// Index of the package in the batch that failed
  public let index: Int

  /// Error message
  public let error: String

  /// Optional error code
  public let code: String?

  public init(index: Int, error: String, code: String? = nil) {
    self.index = index
    self.error = error
    self.code = code
  }
}

/// Data structure for individual key package in batch upload
/// Note: Renamed from KeyPackageData to avoid conflict with uniffi-generated type in MLSFFI.swift
public struct MLSKeyPackageUploadData: Codable, Sendable {
  /// Base64-encoded key package bytes (TLS serialized)
  public let keyPackage: String

  /// Cipher suite identifier
  public let cipherSuite: String

  /// Expiration timestamp
  public let expires: Date?

  /// Idempotency key for deduplication
  public let idempotencyKey: String

  /// Device ID for multi-device support
  public let deviceId: String?

  /// Credential DID (did:plc:user#device-uuid) for multi-device support
  public let credentialDid: String?

  public init(
    keyPackage: String,
    cipherSuite: String = "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519",
    expires: Date? = nil,
    idempotencyKey: String = UUID().uuidString,
    deviceId: String? = nil,
    credentialDid: String? = nil
  ) {
    self.keyPackage = keyPackage
    self.cipherSuite = cipherSuite
    self.expires = expires
    self.idempotencyKey = idempotencyKey
    self.deviceId = deviceId
    self.credentialDid = credentialDid
  }
}

/// Request payload for batch key package upload
public struct KeyPackageBatchUploadRequest: Codable, Sendable {
  /// Array of key packages to upload
  public let keyPackages: [MLSKeyPackageUploadData]

  public init(keyPackages: [MLSKeyPackageUploadData]) {
    self.keyPackages = keyPackages
  }
}
