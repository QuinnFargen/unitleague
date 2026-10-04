import SwiftUI

/// The signed-in bettor's unit history: per-syndicate runner balances and per-league balances.
/// Pushed from `TabProfileView`.
struct ViewPerformance: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId") private var bettorId: Int = 0

    @State private var syndicateRunners: [Runner] = []
    @State private var syndicates: [Syndicate] = []
    @State private var leagueBalances: [BettorLeagueBalance] = []
    @State private var leagues: [League] = []
    @State private var isLoading = true

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            ScrollView {
                VStack(spacing: 24) {
                    if isLoading {
                        ProgressView()
                            .frame(maxWidth: .infinity)
                            .padding(.top, 20)
                    } else if syndicateRunners.isEmpty && leagueBalances.isEmpty {
                        Text("No performance yet. Join a syndicate and place a bet to get started.")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                            .frame(maxWidth: .infinity)
                            .padding(.top, 20)
                    } else {
                        CardUnitBreakdown(
                            syndicateRunners: syndicateRunners,
                            syndicates: syndicates,
                            leagueBalances: leagueBalances,
                            leagues: leagues
                        )
                    }
                }
                .padding(.horizontal, 16)
                .padding(.top, 16)
                .padding(.bottom, 32)
            }
        }
        .navigationTitle("Pick Performance")
        .navigationBarTitleDisplayMode(.inline)
        .task(id: bettorId) {
            await loadData()
        }
    }

    private func loadData() async {
        guard bettorId != 0 else { isLoading = false; return }
        async let runnersFetch = try? RunnerService().fetchRunner(bettorId: bettorId)
        async let syndicatesFetch = try? SyndicateService().fetchSyndicate(bettorId: bettorId)
        async let leagueBalancesFetch = try? BettorService().fetchLeagueBalances(bettorId: bettorId)
        async let leaguesFetch = try? LeagueService().fetchLeagues()
        syndicateRunners = await runnersFetch ?? []
        syndicates = await syndicatesFetch ?? []
        leagueBalances = await leagueBalancesFetch ?? []
        leagues = await leaguesFetch ?? []
        isLoading = false
    }
}

#Preview {
    NavigationStack {
        ViewPerformance()
    }
    .environmentObject(AppTheme())
}
