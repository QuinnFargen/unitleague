import SwiftUI

struct CardOddSingle: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let odd: Odds
    let betType: String

    private let colW: CGFloat = 58

    private let timeInputFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withColonSeparatorInTimeZone]
        return f
    }()

    private let timeOutputFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "h:mm a"
        return f
    }()

    private var formattedTime: String? {
        guard let raw = odd.gameTime,
              let date = timeInputFormatter.date(from: raw) else { return nil }
        return timeOutputFormatter.string(from: date)
    }

    private func formatPrice(_ price: Double) -> String {
        String(format: "%.2f", price)
    }

    private func formatPoints(_ points: Double) -> String {
        points == points.rounded() ? "\(Int(points))" : String(format: "%.1f", points)
    }

    private func impliedPct(_ price: Double?) -> String {
        guard let p = price, p > 0 else { return "" }
        return "\(Int((1.0 / p * 100.0).rounded()))%"
    }

    private func oddsCapsuleColor(_ price: Double, betHash: String?, won: Bool?) -> Color {
        guard betHash != nil else { return theme.accent.opacity(0.2) }
        if let won { return won ? theme.accent.opacity(0.7) : theme.chipUnselected(colorScheme) }
        let distance = min(abs(price - 2.0) * 0.5, 0.85)
        let base = price < 2.0 ? theme.win : theme.loss
        return base.opacity(0.15 + distance)
    }

    @ViewBuilder
    private func priceCapsule(_ price: Double?, subtitle: String = "", betHash: String? = nil, won: Bool? = nil) -> some View {
        if let p = price {
            VStack(spacing: 1) {
                Text(formatPrice(p))
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                if !subtitle.isEmpty {
                    Text(subtitle)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: colW)
            .background(oddsCapsuleColor(p, betHash: betHash, won: won))
            .clipShape(Capsule())
        } else {
            Text("—")
                .font(.caption)
                .foregroundStyle(.secondary)
                .frame(width: colW)
        }
    }

    var body: some View {
        singleModeLayout
            .padding()
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    private struct SingleData {
        let awayPrice: Double?
        let awayBetLabel: String
        let awayMLPct: String
        let awayBetHash: String?
        let awayWon: Bool?
        let homePrice: Double?
        let homeBetLabel: String
        let homeMLPct: String
        let homeBetHash: String?
        let homeWon: Bool?
    }

    private func singleData() -> SingleData {
        switch betType {
        case "ML":
            return SingleData(
                awayPrice: odd.mlAwayPrice, awayBetLabel: "", awayMLPct: impliedPct(odd.mlAwayPrice),
                awayBetHash: odd.mlAwayBetHash, awayWon: odd.mlAwayWon,
                homePrice: odd.mlHomePrice, homeBetLabel: "", homeMLPct: impliedPct(odd.mlHomePrice),
                homeBetHash: odd.mlHomeBetHash, homeWon: odd.mlHomeWon
            )
        case "SPR":
            return SingleData(
                awayPrice: odd.sprAwayPrice,
                awayBetLabel: odd.sprAwayPoints.map(formatPoints) ?? "",
                awayMLPct: impliedPct(odd.sprAwayPrice),
                awayBetHash: odd.sprAwayBetHash, awayWon: odd.sprAwayWon,
                homePrice: odd.sprHomePrice,
                homeBetLabel: odd.sprHomePoints.map(formatPoints) ?? "",
                homeMLPct: impliedPct(odd.sprHomePrice),
                homeBetHash: odd.sprHomeBetHash, homeWon: odd.sprHomeWon
            )
        case "O/U":
            let total = (odd.overPoints ?? odd.underPoints).map(formatPoints) ?? ""
            return SingleData(
                awayPrice: odd.overPrice, awayBetLabel: "O \(total)", awayMLPct: impliedPct(odd.overPrice),
                awayBetHash: odd.overBetHash, awayWon: odd.overWon,
                homePrice: odd.underPrice, homeBetLabel: "U \(total)", homeMLPct: impliedPct(odd.underPrice),
                homeBetHash: odd.underBetHash, homeWon: odd.underWon
            )
        default:
            return SingleData(awayPrice: nil, awayBetLabel: "", awayMLPct: "", awayBetHash: nil, awayWon: nil,
                              homePrice: nil, homeBetLabel: "", homeMLPct: "", homeBetHash: nil, homeWon: nil)
        }
    }

    @ViewBuilder
    private var singleModeLayout: some View {
        let d = singleData()
        HStack(spacing: 8) {
            Image(systemName: odd.sportIcon)
                .font(.title2)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 28)

            VStack(spacing: 2) {
                priceCapsule(d.awayPrice, betHash: d.awayBetHash, won: d.awayWon)
                if !d.awayBetLabel.isEmpty {
                    Text(d.awayBetLabel)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }

            if !d.awayMLPct.isEmpty {
                Text(d.awayMLPct)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            VStack(spacing: 2) {
                Text(odd.awayAbbr + " @ " + odd.homeAbbr)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .multilineTextAlignment(.center)
                if let time = formattedTime {
                    Text(time).font(.caption2).foregroundStyle(.secondary)
                }
            }
            .frame(maxWidth: .infinity)

            if !d.homeMLPct.isEmpty {
                Text(d.homeMLPct)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            VStack(spacing: 2) {
                priceCapsule(d.homePrice, betHash: d.homeBetHash, won: d.homeWon)
                if !d.homeBetLabel.isEmpty {
                    Text(d.homeBetLabel)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }
}

#Preview("CardOddSingle") {
    VStack(spacing: 12) {
        CardOddSingle(odd: Mock.odds, betType: "ML")
        CardOddSingle(odd: Mock.odds, betType: "SPR")
        CardOddSingle(odd: Mock.odds, betType: "O/U")
    }
    .padding()
    .environmentObject(AppTheme())
}
