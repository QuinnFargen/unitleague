import SwiftUI

struct CardGame: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let game: Game

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

    var body: some View {
        HStack {
            Image(systemName: game.sportIcon)
                .font(.title2)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 36, height: 36)
                .background(theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 8))

            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 4) {
                    Text(game.away)
                        .foregroundStyle(game.winner == game.away ? theme.win : theme.primaryText(colorScheme))
                    Text("@")
                        .foregroundStyle(.secondary)
                    Text(game.home)
                        .foregroundStyle(game.winner == game.home ? theme.win : theme.primaryText(colorScheme))
                }
                .font(.headline)
            }

            Spacer()

            if let hscore = game.homeScore, let ascore = game.awayScore {
                VStack(alignment: .trailing, spacing: 2) {
                    Text("\(ascore) – \(hscore)")
                        .font(.headline)
                        .foregroundStyle(theme.primaryText(colorScheme))
                    Text("FINAL")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            } else if let time = formattedTime {
                Text(time)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            } else {
                Text("TBD")
                    .font(.subheadline)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardGame") {
    VStack(spacing: 12) {
        CardGame(game: Mock.gameLive)
        CardGame(game: Mock.gameUpcoming)
    }
    .padding()
    .environmentObject(AppTheme())
}
