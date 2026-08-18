import SwiftUI

/// Shows a runner's profile card and cross-syndicate/league unit-history breakdown. Reached from
/// the pickleball-icon button on an opponent's `CardProfile` in `SheetRunner` — kept out of
/// `SheetRunner`'s default view so a syndicate mate's broader profile is one tap away rather than
/// shown by default.
struct SheetProfile: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    let title: String
    let symbol: String?
    let color: String?
    let name: String
    var favoriteTeamAbbr: String? = nil
    var favoriteLeagueId: Int? = nil
    var careerUnits: Double? = nil
    let syndicateRunners: [Runner]
    var syndicates: [Syndicate] = []
    let leagueBalances: [BettorLeagueBalance]
    let leagues: [League]

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 20) {
                        CardProfile(
                            symbol: symbol,
                            color: color,
                            name: name,
                            favoriteTeamAbbr: favoriteTeamAbbr,
                            favoriteLeagueId: favoriteLeagueId,
                            careerUnits: careerUnits
                        )

                        CardUnitBreakdown(
                            syndicateRunners: syndicateRunners,
                            syndicates: syndicates,
                            leagueBalances: leagueBalances,
                            leagues: leagues
                        )
                    }
                    .padding(16)
                }
            }
            .navigationTitle(title)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}

#Preview("SheetProfile") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetProfile(
            title: "Quinn",
            symbol: "figure.american.football.circle.fill",
            color: "green",
            name: "Quinn",
            favoriteTeamAbbr: "KC",
            favoriteLeagueId: 2,
            careerUnits: 128,
            syndicateRunners: Mock.runners,
            syndicates: [Mock.syndicate],
            leagueBalances: [
                BettorLeagueBalance(bettorId: 42, leagueId: 2, balance: 42),
                BettorLeagueBalance(bettorId: 42, leagueId: 1, balance: -12)
            ],
            leagues: [Mock.leagueNBA, Mock.leagueNFL]
        )
        .environmentObject(AppTheme())
    }
}
