import Foundation

@MainActor
public protocol MLSStorageMaintenanceCoordinating: AnyObject {
  func beginStorageMaintenance(for userDID: String)
  func endStorageMaintenance(for userDID: String)
  func prepareMLSStorageReset(for userDID: String) async
}
