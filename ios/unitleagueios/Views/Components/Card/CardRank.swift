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

            VStack(alignment: .leading, spacing: 2) {
                Text(season.abbr)
                    .font(.headline)
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let streak = season.last5Str {
                    Text("Last 5: \(streak)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 2) {
                Text("\(season.wins)-\(season.losses)")
                    .font(.headline)
                    .foregroundStyle(theme.primaryText(colorScheme))
                Text(String(format: "%.3f", season.winPct))
                    .font(.caption2)
                    .foregroundStyle(.secondary)
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
