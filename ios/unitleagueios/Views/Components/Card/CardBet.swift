import SwiftUI

struct CardBet: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let bet: SelectedBet
    var won: Bool? = nil
    var priceMultiplier: Double? = nil
    var showTime: Bool = true
    var showEnhanced: Bool = true

    private let timeInputFmt: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withColonSeparatorInTimeZone]
        return f
    }()
    private let timeOutputFmt: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "h:mm a"
        return f
    }()

    private var formattedTime: String? {
        guard let raw = bet.gameTime, let d = timeInputFmt.date(from: raw) else { return nil }
        return timeOutputFmt.string(from: d)
    }

    /// Pre-placement odds-browsing bets use `type == "O/U"` with `side == "Over"/"Under"`;
    /// placed/graded bets use `type == "OVER"/"UNDER"` directly.
    private var isOverUnder: Bool { bet.type == "O/U" || bet.type == "OVER" || bet.type == "UNDER" }
    private var isOver: Bool { bet.type == "OVER" || bet.side == "Over" }

    private func teamCapsule(_ text: String) -> some View {
        Text(text)
            .padding(.horizontal, 8)
            .padding(.vertical, 2)
            .background(theme.cardBackgroundProminent(colorScheme))
            .clipShape(Capsule())
    }

    private func capsuleText(_ abbr: String) -> String {
        guard abbr == bet.team else { return abbr }
        switch bet.type {
        case "SPR":
            if let p = bet.points { return "\(abbr) \(OddsFormatting.formatPointsSigned(p))" }
            return "\(abbr) SPR"
        default:
            return abbr
        }
    }

    @ViewBuilder
    private var ouCapsule: some View {
        if isOverUnder, let p = bet.points {
            HStack(spacing: 4) {
                Image(systemName: isOver ? "arrow.up.circle.fill" : "arrow.down.circle.fill")
                Text(OddsFormatting.formatPoints(p))
            }
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 2)
            .background(theme.cardBackgroundProminent(colorScheme))
            .clipShape(Capsule())
        }
    }

    @ViewBuilder
    private var matchupLine: some View {
        HStack(spacing: 4) {
            if (bet.type == "ML" || bet.type == "SPR"), let team = bet.team, team == bet.awayAbbr {
                teamCapsule(capsuleText(bet.awayAbbr))
                Text("@ " + bet.homeAbbr)
            } else if (bet.type == "ML" || bet.type == "SPR"), let team = bet.team, team == bet.homeAbbr {
                Text(bet.awayAbbr + " @")
                teamCapsule(capsuleText(bet.homeAbbr))
            } else {
                Text(bet.awayAbbr + " @ " + bet.homeAbbr)
            }
            ouCapsule
        }
        .font(.headline)
        .foregroundStyle(theme.primaryText(colorScheme))
    }

    @ViewBuilder
    private var scoreOrTimeRow: some View {
        if let hscore = bet.homeScore, let ascore = bet.awayScore {
            HStack(spacing: 4) {
                Text("\(ascore)")
                Text("–")
                Text("\(hscore)")
            }
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
        } else if showTime, let time = formattedTime {
            Text(time)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    var body: some View {
        HStack(spacing: 16) {
            VStack(alignment: .leading, spacing: 4) {
                matchupLine
                scoreOrTimeRow
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 3) {
                PriceUnitsBlock(
                    price: bet.price,
                    unit: bet.unit,
                    won: won,
                    priceEnhanced: bet.priceEnhanced,
                    unitEnhanced: bet.unitEnhanced,
                    showEnhanced: showEnhanced
                )
                if let priceMultiplier {
                    Text(String(format: "%.2fx", priceMultiplier))
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(theme.accent)
                }
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardBet") {
    VStack(spacing: 12) {
        CardBet(bet: Mock.selectedBetML)
        CardBet(bet: Mock.selectedBetSPR)
        CardBet(bet: Mock.selectedBetOU)
    }
    .padding()
    .environmentObject(AppTheme())
}
