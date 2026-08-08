import Foundation

enum OddsFormatting {
    static func formatPrice(_ price: Double) -> String {
        String(format: "%.2f", price)
    }

    static func formatPoints(_ points: Double) -> String {
        points == points.rounded() ? "\(Int(points))" : String(format: "%.1f", points)
    }

    static func formatPointsSigned(_ points: Double) -> String {
        points == points.rounded()
            ? (points >= 0 ? "+\(Int(points))" : "\(Int(points))")
            : String(format: points >= 0 ? "+%.1f" : "%.1f", points)
    }

    static func impliedPct(_ price: Double?) -> String {
        guard let p = price, p > 0 else { return "" }
        return "\(Int((1.0 / p * 100.0).rounded()))%"
    }
}
