// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "CatbirdMLSService",
    platforms: [
        .iOS(.v18),
        .macOS(.v15)
    ],
    products: [
        .library(
            name: "CatbirdMLSService",
            targets: ["CatbirdMLSService"]
        )
    ],
    dependencies: [
        .package(path: "../CatbirdMLSCore"),
        .package(path: "../Petrel"),
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.0.0"),
        .package(url: "https://github.com/valpackett/SwiftCBOR.git", .upToNextMajor(from: "0.5.0"))
    ],
    targets: [
        .target(
            name: "CatbirdMLSService",
            dependencies: [
                "CatbirdMLSCore",
                "Petrel",
                "SwiftCBOR",
                .product(name: "GRDB", package: "GRDB.swift")
            ],
            swiftSettings: [
                // Keep Swift 5 mode to match app targets.
                .swiftLanguageMode(.v5)
            ],
            linkerSettings: [
                .linkedFramework("Security")
            ]
        )
    ]
)
