import SwiftUI

/// Shows another syndicate runner's profile card, unit-history breakdown, and just their own
/// bet history. Opened by tapping a row in `ViewSyndicate`'s Standing list.
struct SheetRunner: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    let runner: Runner

    @State private var stats: BettorStats?
    @State private var syndicateRunners: [Runner] = []
    @State private var leagueBalances: [BettorLeagueBalance] = []
    @State private var leagues: [League] = []
    @State private var completedBets: [Txn] = []
    @State private var isLoading = false

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                        .frame(maxWidth: .infinity)
                        .padding(.top, 40)
                } else {
                    ScrollView {
                        VStack(spacing: 20) {
                            CardProfile(
                                symbol: runner.symbol,
                                color: runner.color,
                                name: runner.profileName ?? "Runner",
                                favoriteTeamAbbr: stats?.favoriteTeamAbbr,
                                favoriteLeagueId: stats?.favoriteLeagueId,
                                careerUnits: stats?.careerBalance
                            )

                            CardUnitBreakdown(
                                syndicateRunners: syndicateRunners,
                                leagueBalances: leagueBalances,
                                leagues: leagues
                            )

                            if !completedBets.isEmpty {
                                VStack(alignment: .leading, spacing: 8) {
                                    Text("Bet History")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(.secondary)
                                        .padding(.horizontal, 4)

                                    VStack(spacing: 8) {
                                        ForEach(completedBets) { txn in
                                            betRow(txn)
                                        }
                                    }
                                }
                            }
                        }
                        .padding(16)
                    }
                }
            }
            .navigationTitle(runner.profileName ?? "Runner")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .task { await load() }
        }
    }

    @ViewBuilder
    private func betRow(_ txn: Txn) -> some View {
        let card = CardBetSlim(txn: txn, runner: runner)
        if let gameId = txn.gameId {
            NavigationLink {
                ViewGameDetailLoader(gameId: gameId)
            } label: {
                card
            }
            .buttonStyle(.plain)
        } else {
            card
        }
    }

    private func load() async {
        isLoading = true
        async let statsFetch = try? BettorService().fetchStats(bettorId: runner.bettorId)
        async let runnersFetch = try? RunnerService().fetchRunner(bettorId: runner.bettorId)
        async let leagueBalancesFetch = try? BettorService().fetchLeagueBalances(bettorId: runner.bettorId)
        async let leaguesFetch = try? LeagueService().fetchLeagues()
        async let betsFetch = try? TxnService().fetchCompletedBets(bettorId: runner.bettorId)
        stats = await statsFetch ?? nil
        syndicateRunners = await runnersFetch ?? []
        leagueBalances = await leagueBalancesFetch ?? []
        leagues = await leaguesFetch ?? []
        completedBets = await betsFetch ?? []
        isLoading = false
    }
}

#Preview("SheetRunner") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetRunner(runner: Mock.runnerAdmin)
            .environmentObject(AppTheme())
    }
}
