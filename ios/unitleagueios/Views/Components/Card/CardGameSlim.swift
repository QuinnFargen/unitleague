import SwiftUI

struct CardGameSlim: View {
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
        HStack(spacing: 8) {
            HStack(spacing: 4) {
                Text(game.away)
                    .foregroundStyle(game.winner == game.away ? theme.win : theme.primaryText(colorScheme))
                Text("@")
                    .foregroundStyle(.secondary)
                Text(game.home)
                    .foregroundStyle(game.winner == game.home ? theme.win : theme.primaryText(colorScheme))
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
}

#Preview("CardGameSlim") {
    VStack(spacing: 8) {
        CardGameSlim(game: Mock.gameLive)
        CardGameSlim(game: Mock.gameUpcoming)
    }
    .padding()
    .environmentObject(AppTheme())
}
