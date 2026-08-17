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

    /// Only shown when the game hasn't finished — once there's a score, `scoreRow` takes over.
    @ViewBuilder
    private var timeRow: some View {
        if bet.homeScore == nil, showTime, let time = formattedTime {
            Text(time)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    /// Sits right next to the matchup once the game has a final score — see `matchupAndScore`,
    /// which drops it to its own line below the matchup if the row is too tight for both.
    @ViewBuilder
    private var scoreRow: some View {
        if let hscore = bet.homeScore, let ascore = bet.awayScore {
            HStack(spacing: 4) {
                Text("\(ascore)")
                Text("–")
                Text("\(hscore)")
            }
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
        }
    }

    /// Keeps the score close to the team names (not crowding the price/units block) by trying
    /// it inline first, and only wrapping it to a second line when the row is too narrow —
    /// e.g. long team-capsule text — instead of letting individual numbers break mid-word.
    @ViewBuilder
    private var matchupAndScore: some View {
        if bet.homeScore != nil {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    matchupLine
                    scoreRow
                }
                VStack(alignment: .leading, spacing: 2) {
                    matchupLine
                    scoreRow
                }
            }
        } else {
            matchupLine
        }
    }

    var body: some View {
        HStack(spacing: 16) {
            VStack(alignment: .leading, spacing: 4) {
                matchupAndScore
                timeRow
            }

            Spacer(minLength: 8)

            VStack(alignment: .trailing, spacing: 3) {
                HStack(spacing: 3) {
                    CardPriceUnits(
                        price: bet.price,
                        unit: bet.unit,
                        won: won,
                        priceEnhanced: bet.priceEnhanced,
                        unitEnhanced: bet.unitEnhanced,
                        showEnhanced: showEnhanced
                    )
                    if bet.unit == nil {
                        Text("x")
                            .font(.headline)
                            .foregroundStyle(theme.primaryText(colorScheme))
                    }
                }
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
