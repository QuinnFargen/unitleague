import SwiftUI

struct CardGame: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let game: Game
    var odds: Odds? = nil

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
        guard let raw = game.gameTime,
              let date = timeInputFormatter.date(from: raw) else { return nil }
        return timeOutputFormatter.string(from: date)
    }

    /// nil when no odds are available; true when away is favored, false when home is favored.
    private var awayIsFav: Bool? {
        guard let away = odds?.sprAwayPoints else { return nil }
        return away < 0
    }

    private var totalPoints: Double? { odds?.overPoints ?? odds?.underPoints }

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: game.sportIcon)
                .font(.caption)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 16)

            HStack(spacing: 4) {
                Text(game.away)
                    .foregroundStyle(game.winner == game.away ? theme.accent : theme.primaryText(colorScheme))
                    .frame(width: 34, alignment: .leading)
                Text("@")
                    .foregroundStyle(.secondary)
                Text(game.home)
                    .foregroundStyle(game.winner == game.home ? theme.accent : theme.primaryText(colorScheme))
                    .frame(width: 34, alignment: .leading)
                if let oddsGroup = oddsGroupText {
                    oddsGroup
                        .font(.caption2.weight(.semibold))
                }
            }
            .font(.subheadline.weight(.semibold))

            Spacer(minLength: 8)

            if let hscore = game.homeScore, let ascore = game.awayScore {
                HStack(alignment: .firstTextBaseline, spacing: 0) {
                    Text("\(ascore)")
                        .font(.subheadline.weight(.bold))
                        .monospacedDigit()
                        .foregroundStyle(game.winner == game.away ? theme.accent : theme.primaryText(colorScheme))
                        .frame(width: 28, alignment: .trailing)
                    Text(" – ")
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                    Text("\(hscore)")
                        .font(.subheadline.weight(.bold))
                        .monospacedDigit()
                        .foregroundStyle(game.winner == game.home ? theme.accent : theme.primaryText(colorScheme))
                        .frame(width: 28, alignment: .leading)
                }
            } else if let time = formattedTime {
                Text(time)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("TBD")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }

    /// Single combined parenthetical: away's own stat (spread if favored, else implied ML%),
    /// home's own stat (mirrored), then the total O/U line — each colored independently by
    /// whether that particular bet won.
    private var oddsGroupText: Text? {
        guard let odds else { return nil }
        let parts = [awayOddsText(odds), homeOddsText(odds), totalOddsText(odds)].compactMap { $0 }
        guard !parts.isEmpty else { return nil }
        let joined = parts.dropFirst().reduce(parts[0]) { acc, next in
            acc + Text(", ").foregroundColor(.secondary) + next
        }
        return Text("(").foregroundColor(.secondary) + joined + Text(")").foregroundColor(.secondary)
    }

    private func awayOddsText(_ odds: Odds) -> Text? {
        if awayIsFav == true, let pts = odds.sprAwayPoints {
            return Text(OddsFormatting.formatPointsSigned(pts))
                .foregroundColor(odds.sprAwayWon == true ? theme.accent : .secondary)
        } else if awayIsFav == false {
            return Text(OddsFormatting.impliedPct(odds.mlAwayPrice))
                .foregroundColor(odds.mlAwayWon == true ? theme.accent : .secondary)
        }
        return nil
    }

    private func homeOddsText(_ odds: Odds) -> Text? {
        if awayIsFav == false, let pts = odds.sprHomePoints {
            return Text(OddsFormatting.formatPointsSigned(pts))
                .foregroundColor(odds.sprHomeWon == true ? theme.accent : .secondary)
        } else if awayIsFav == true {
            return Text(OddsFormatting.impliedPct(odds.mlHomePrice))
                .foregroundColor(odds.mlHomeWon == true ? theme.accent : .secondary)
        }
        return nil
    }

    private func totalOddsText(_ odds: Odds) -> Text? {
        guard let total = totalPoints else { return nil }
        return Text(OddsFormatting.formatPoints(total))
            .foregroundColor(odds.overWon == true ? theme.accent : .secondary)
    }
}

#Preview("CardGame") {
    VStack(spacing: 8) {
        CardGame(game: Mock.gameLive, odds: Mock.oddsCompleted)
        CardGame(game: Mock.gameUpcoming, odds: Mock.odds)
        CardGame(game: Mock.gameUpcoming)
    }
    .padding()
    .environmentObject(AppTheme())
}
