import SwiftUI

struct CardTeam: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let team: Team
    let league: League
    var showChevron: Bool = false

    var body: some View {
        ZStack(alignment: .bottomLeading) {
            LinearGradient(
                colors: [team.teamColor.opacity(0.35), Color.secondary],
                startPoint: .top,
                endPoint: .bottom
            )
            .frame(maxWidth: .infinity)
            .frame(height: 80)

            HStack(alignment: .center, spacing: 14) {
                Image(systemName: league.sportIcon)
                    .font(.system(size: 28, weight: .semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .frame(width: 48, height: 48)
                    .background(theme.cardBackgroundProminent(colorScheme))
                    .clipShape(RoundedRectangle(cornerRadius: 12))

                VStack(alignment: .leading, spacing: 3) {
                    HStack(alignment: .firstTextBaseline, spacing: 8) {
                        Text(team.abbr)
                            .font(.title2).bold()
                            .foregroundStyle(theme.primaryText(colorScheme))
                        Text(team.name)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }

                    TeamMetaRow(team: team)
                }

                Spacer()

                if showChevron {
                    Image(systemName: "chevron.right")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            .padding(.horizontal)
            .padding(.bottom, 14)
        }
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

// MARK: - TeamMetaRow

struct TeamMetaRow: View {
    let team: Team

    var body: some View {
        let confDiv = [team.conf, team.div].compactMap { $0 }.joined(separator: " · ")
        HStack(spacing: 6) {
            if !confDiv.isEmpty {
                Text(confDiv)
            }
            if team.region != nil {
                if !confDiv.isEmpty { Text("·").foregroundStyle(.tertiary) }
                Image(systemName: team.regionIcon)
            }
            if let mascot = team.mascot {
                Label(mascot, systemImage: team.categoryIcon)
            }
        }
        .font(.caption)
        .foregroundStyle(.secondary)
    }
}

#Preview("ViewTeamBanner") {
    VStack(spacing: 16) {
        CardTeam(team: Mock.teamLAL, league: Mock.leagueNBA)
        CardTeam(team: Mock.teamBOS, league: Mock.leagueNBA, showChevron: true)
    }
    .padding()
    .environmentObject(AppTheme())
}
