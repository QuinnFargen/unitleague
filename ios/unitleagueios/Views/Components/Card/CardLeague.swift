import SwiftUI

struct CardLeague: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    var body: some View {
        HStack(spacing: 16) {
            Image(systemName: league.sportIcon)
                .font(.title2)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 44, height: 44)
                .background(theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 10))

            Text(league.abbr)
                .font(.title2.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
                .multilineTextAlignment(.center)
                .frame(width: 52, alignment: .center)

            Spacer()

            HStack(spacing: 10) {
                NavigationLink {
                    ViewTeamList(league: league)
                } label: {
                    LeagueOptionIcon(icon: "person.2")
                }
                .buttonStyle(.plain)

                NavigationLink {
                    ViewLeagueSched(league: league)
                } label: {
                    LeagueOptionIcon(icon: "calendar")
                }
                .buttonStyle(.plain)

                NavigationLink {
                    ViewRank(league: league)
                } label: {
                    LeagueOptionIcon(icon: "list.number")
                }
                .buttonStyle(.plain)

                NavigationLink {
                    ViewOdds(league: league)
                } label: {
                    LeagueOptionIcon(icon: "chart.bar")
                }
                .buttonStyle(.plain)
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

// MARK: - LeagueOptionIcon

private struct LeagueOptionIcon: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let icon: String

    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 8)
                .fill(theme.appBackground(colorScheme))
                .frame(width: 36, height: 36)
            Image(systemName: icon)
                .font(.subheadline)
                .foregroundStyle(theme.accent)
        }
        .frame(width: 36, height: 36)
    }
}

#Preview("CardLeague") {
    VStack(spacing: 12) {
        CardLeague(league: Mock.leagueNBA)
        CardLeague(league: Mock.leagueNFL)
    }
    .padding()
    .environmentObject(AppTheme())
}
