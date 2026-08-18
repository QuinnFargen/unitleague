import SwiftUI

/// Shows a runner's cross-syndicate/league unit-history breakdown. Opened from the "User"
/// capsule in `SheetRunner` — kept out of `SheetRunner`'s default view so a syndicate mate's
/// broader profile is one tap away rather than shown by default.
struct SheetProfile: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    let title: String
    let syndicateRunners: [Runner]
    let leagueBalances: [BettorLeagueBalance]
    let leagues: [League]

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    CardUnitBreakdown(
                        syndicateRunners: syndicateRunners,
                        leagueBalances: leagueBalances,
                        leagues: leagues
                    )
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
            syndicateRunners: Mock.runners,
            leagueBalances: [
                BettorLeagueBalance(bettorId: 42, leagueId: 2, balance: 42),
                BettorLeagueBalance(bettorId: 42, leagueId: 1, balance: -12)
            ],
            leagues: [Mock.leagueNBA, Mock.leagueNFL]
        )
        .environmentObject(AppTheme())
    }
}
