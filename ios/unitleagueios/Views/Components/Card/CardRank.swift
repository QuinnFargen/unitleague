import SwiftUI

struct CardRank: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let rank: Int
    let season: TeamSeason

    var body: some View {
        HStack {
            Text("\(rank)")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 24, alignment: .leading)

            Text(season.abbr)
                .font(.headline)
                .foregroundStyle(theme.primaryText(colorScheme))

            HStack(spacing: 4) {
                Text("\(season.wins)-\(season.losses)")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                Text("(\(String(format: "%.3f", season.winPct)))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            if let streak = season.last10Str {
                StreakText(streak: streak, positiveChar: "W")
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardRank") {
    VStack(spacing: 12) {
        CardRank(rank: 1, season: Mock.teamSeasonLAL)
        CardRank(rank: 2, season: Mock.teamSeasonBOS)
    }
    .padding()
    .environmentObject(AppTheme())
}
