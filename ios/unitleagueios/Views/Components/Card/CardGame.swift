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
                    .lineLimit(1)
                    .frame(width: 50, alignment: .leading)
                Text("@")
                    .foregroundStyle(.secondary)
                Text(game.home)
                    .foregroundStyle(game.winner == game.home ? theme.accent : theme.primaryText(colorScheme))
                    .lineLimit(1)
                    .frame(width: 50, alignment: .leading)
                oddsGroupView
                    .font(.caption2.weight(.semibold))
            }
            .font(.subheadline.weight(.semibold))

            Spacer(minLength: 8)

            if let hscore = game.homeScore, let ascore = game.awayScore {
                HStack(alignment: .firstTextBaseline, spacing: 0) {
                    Text("\(ascore)")
                        .font(.subheadline.weight(.bold))
                        .monospacedDigit()
                        .foregroundStyle(game.winner == game.away ? theme.accent : theme.primaryText(colorScheme))
                        .lineLimit(1)
                        .frame(width: 32, alignment: .trailing)
                    Text(" – ")
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                    Text("\(hscore)")
                        .font(.subheadline.weight(.bold))
                        .monospacedDigit()
                        .foregroundStyle(game.winner == game.home ? theme.accent : theme.primaryText(colorScheme))
                        .lineLimit(1)
                        .frame(width: 32, alignment: .leading)
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

    private struct OddsSegment {
        let text: Text
        let width: CGFloat
    }

    /// Home team's implied ML%, home team's spread, then the total O/U line — fixed-width
    /// columns so the values line up vertically across a schedule of many cards.
    private var oddsSegments: [OddsSegment]? {
        guard let odds else { return nil }
        var segments: [OddsSegment] = []
        if let pct = homePctText(odds) {
            segments.append(OddsSegment(text: pct, width: 30))
        }
        if let spread = homeSpreadText(odds) {
            segments.append(OddsSegment(text: spread, width: 40))
        }
        if let total = totalOddsText(odds) {
            segments.append(OddsSegment(text: total, width: 40))
        }
        return segments.isEmpty ? nil : segments
    }

    @ViewBuilder
    private var oddsGroupView: some View {
        if let segments = oddsSegments {
            HStack(spacing: 0) {
                ForEach(Array(segments.enumerated()), id: \.offset) { index, segment in
                    if index > 0 {
                        Text("|")
                            .foregroundColor(.secondary)
                            .frame(width: 8, alignment: .center)
                    }
                    segment.text
                        .monospacedDigit()
                        .frame(width: segment.width, alignment: .trailing)
                }
            }
        }
    }

    private func homePctText(_ odds: Odds) -> Text? {
        guard let price = odds.mlHomePrice else { return nil }
        let formatted = OddsFormatting.impliedPct(price)
        guard !formatted.isEmpty else { return nil }
        return Text(formatted)
            .foregroundColor(odds.mlHomeWon == true ? theme.accent : .secondary)
    }

    private func homeSpreadText(_ odds: Odds) -> Text? {
        guard let pts = odds.sprHomePoints else { return nil }
        return Text(OddsFormatting.formatPointsSigned(pts))
            .foregroundColor(odds.sprHomeWon == true ? theme.accent : .secondary)
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
