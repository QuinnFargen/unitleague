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
                .font(.title)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 34)

            HStack(spacing: 4) {
                awayLabel
                Text("@")
                    .foregroundStyle(.secondary)
                homeLabel
                if let total = totalPoints {
                    Text("[\(OddsFormatting.formatPoints(total))]")
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(odds?.overWon == true ? theme.win : .secondary)
                }
            }
            .font(.subheadline.weight(.semibold))

            Spacer(minLength: 8)

            if let hscore = game.homeScore, let ascore = game.awayScore {
                HStack(spacing: 6) {
                    Text("\(ascore) – \(hscore)")
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                    Text("FINAL")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
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

    @ViewBuilder
    private var awayLabel: some View {
        HStack(spacing: 2) {
            Text(game.away)
                .foregroundStyle(game.winner == game.away ? theme.win : theme.primaryText(colorScheme))
            if awayIsFav == true, let pts = odds?.sprAwayPoints {
                Text("(\(OddsFormatting.formatPointsSigned(pts)))")
                    .font(.caption2)
                    .foregroundStyle(odds?.sprAwayWon == true ? theme.win : .secondary)
            } else if awayIsFav == false {
                Text("(\(OddsFormatting.impliedPct(odds?.mlAwayPrice)))")
                    .font(.caption2)
                    .foregroundStyle(odds?.mlAwayWon == true ? theme.win : .secondary)
            }
        }
    }

    @ViewBuilder
    private var homeLabel: some View {
        HStack(spacing: 2) {
            Text(game.home)
                .foregroundStyle(game.winner == game.home ? theme.win : theme.primaryText(colorScheme))
            if awayIsFav == false, let pts = odds?.sprHomePoints {
                Text("(\(OddsFormatting.formatPointsSigned(pts)))")
                    .font(.caption2)
                    .foregroundStyle(odds?.sprHomeWon == true ? theme.win : .secondary)
            } else if awayIsFav == true {
                Text("(\(OddsFormatting.impliedPct(odds?.mlHomePrice)))")
                    .font(.caption2)
                    .foregroundStyle(odds?.mlHomeWon == true ? theme.win : .secondary)
            }
        }
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
