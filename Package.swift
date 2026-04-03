// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "ElectricSwift",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
    ],
    products: [
        .library(
            name: "ElectricSwift",
            targets: ["ElectricSwift"]
        ),
    ],
    targets: [
        .target(
            name: "ElectricSwift"
        ),
        .testTarget(
            name: "ElectricSwiftTests",
            dependencies: ["ElectricSwift"]
        ),
    ]
)
