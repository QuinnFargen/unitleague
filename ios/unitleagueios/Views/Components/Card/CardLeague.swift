import SwiftUI

struct CardLeague: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League
    let isExpanded: Bool
    let onTap: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            Button(action: onTap) {
                HStack(spacing: 16) {
                    Image(systemName: league.sportIcon)
                        .font(.title2)
                        .foregroundStyle(theme.primaryText(colorScheme))
                        .frame(width: 44, height: 44)
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 10))

                    VStack(alignment: .leading, spacing: 2) {
                        Text(league.abbr)
                            .font(.title2.weight(.semibold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                        Text(league.sport)
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }

                    Spacer()

                    Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: 32, height: 32)
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(Circle())
                }
                .padding()
            }
            .buttonStyle(.plain)

            if isExpanded {
                Divider().background(theme.divider(colorScheme))
                    .padding(.horizontal)

                HStack(spacing: 12) {
                    NavigationLink {
                        ViewTeamList(league: league)
                    } label: {
                        LeagueOptionCell(icon: "person.2", title: "Teams")
                    }
                    .buttonStyle(.plain)

                    LeagueOptionCell(icon: "list.number", title: "Ranks")
                    LeagueOptionCell(icon: "calendar", title: "Sched")
                    LeagueOptionCell(icon: "chart.bar", title: "Odds")
                }
                .padding(.horizontal)
                .padding(.vertical, 12)
            }
        }
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

// MARK: - LeagueOptionCell

private struct LeagueOptionCell: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let icon: String
    let title: String

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            ZStack {
                RoundedRectangle(cornerRadius: 8)
                    .fill(theme.appBackground(colorScheme))
                    .frame(width: 32, height: 32)
                Image(systemName: icon)
                    .font(.title3)
                    .foregroundStyle(theme.accent)
            }
            .frame(width: 32, height: 32)

            Text(title)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }
}

#Preview("CardLeague") {
    VStack(spacing: 12) {
        CardLeague(league: Mock.leagueNBA, isExpanded: false) {}
        CardLeague(league: Mock.leagueNFL, isExpanded: true) {}
    }
    .padding()
    .environmentObject(AppTheme())
}
