import Foundation

package enum ElectricRowPatcher {
    package static func applying(
        patch: ElectricRow,
        to base: ElectricRow
    ) -> ElectricRow {
        base.merging(patch) { _, incoming in incoming }
    }

    package static func applying(
        patch: ElectricRow,
        to base: ElectricRow,
        preserving fields: Set<String>
    ) -> ElectricRow {
        guard fields.isEmpty == false else {
            return applying(patch: patch, to: base)
        }

        var merged = applying(patch: patch, to: base)
        for field in fields {
            if let original = base[field] {
                merged[field] = original
            }
        }
        return merged
    }
}
