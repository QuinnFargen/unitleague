import SwiftUI

struct CardOddsTrend: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let record: TeamOddsRecent

    var body: some View {
        HStack {
            Text(record.abbr)
                .font(.headline)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 56, alignment: .leading)

            Spacer()

            VStack(alignment: .trailing, spacing: 2) {
                Text("\(record.atsWins)-\(record.atsLosses) ATS")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let streak = record.atsLast10Str {
                    Text(streak)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }

            Divider().frame(height: 28)

            VStack(alignment: .trailing, spacing: 2) {
                Text("\(record.overCount) O / \(record.underCount) U")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                Text("Last 10")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardOddsTrend") {
    VStack(spacing: 12) {
        CardOddsTrend(record: Mock.teamOddsRecentLAL)
        CardOddsTrend(record: Mock.teamOddsRecentBOS)
    }
    .padding()
    .environmentObject(AppTheme())
}
