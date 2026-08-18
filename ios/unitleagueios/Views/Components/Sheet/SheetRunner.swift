import SwiftUI

/// Shows a syndicate runner's profile card, their Juice for this syndicate, and just their own
/// bet history. Opened by tapping a row in `ViewSyndicate`'s Standing list. The cross-syndicate
/// unit-history breakdown lives one tap away in `SheetProfile`, reached via a button on
/// `CardProfile`: a pencil that opens `SheetEditProfile` when viewing your own runner, or a
/// pickleball icon that opens `SheetProfile` when viewing an opponent's.
struct SheetRunner: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("bettorId") private var bettorId: Int = 0

    @State private var runner: Runner

    @State private var stats: BettorStats?
    @State private var syndicateRunners: [Runner] = []
    @State private var syndicates: [Syndicate] = []
    @State private var leagueBalances: [BettorLeagueBalance] = []
    @State private var leagues: [League] = []
    @State private var completedBets: [Txn] = []
    @State private var syndicateEnhanced: [Enhanced] = []
    @State private var isLoading = false
    @State private var showingEditRunner = false
    @State private var showingProfile = false
    @StateObject private var weeksStore = SyndicateWeeksStore()
    @State private var selectedWeekId: Int?

    init(runner: Runner) {
        _runner = State(initialValue: runner)
    }

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
                                careerUnits: stats?.careerBalance,
                                isEditable: runner.bettorId == bettorId,
                                onEdit: { showingEditRunner = true },
                                showsViewProfileButton: runner.bettorId != bettorId,
                                onViewProfile: { showingProfile = true }
                            )

                            if !syndicateEnhanced.isEmpty {
                                juiceSection
                            }

                            if !completedBets.isEmpty {
                                VStack(alignment: .leading, spacing: 8) {
                                    HStack {
                                        Text("Bet History")
                                            .font(.caption.weight(.semibold))
                                            .foregroundStyle(.secondary)
                                        Spacer()
                                        if !weeksStore.weeks.isEmpty {
                                            WeekNavigationHeader(weeks: weeksStore.weeks, selectedWeekId: $selectedWeekId)
                                        }
                                    }
                                    .padding(.horizontal, 4)

                                    VStack(spacing: 8) {
                                        ForEach(completedBets.filter { $0.weekId == selectedWeekId }) { txn in
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
            .task { await loadAll() }
            .sheet(isPresented: $showingEditRunner) {
                SheetEditProfile(runner: $runner)
            }
            .sheet(isPresented: $showingProfile) {
                SheetProfile(
                    title: runner.profileName ?? "Runner",
                    symbol: runner.symbol,
                    color: runner.color,
                    name: runner.profileName ?? "Runner",
                    favoriteTeamAbbr: stats?.favoriteTeamAbbr,
                    favoriteLeagueId: stats?.favoriteLeagueId,
                    careerUnits: stats?.careerBalance,
                    syndicateRunners: syndicateRunners,
                    syndicates: syndicates,
                    leagueBalances: leagueBalances,
                    leagues: leagues
                )
            }
        }
    }

    private var juiceSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Juice")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .padding(.horizontal, 4)

            let team = syndicateEnhanced.filter { $0.enhancementType == "team" }.sorted { $0.name < $1.name }
            let clv  = syndicateEnhanced.filter { $0.enhancementType == "clv" }
            let edge = syndicateEnhanced.filter { $0.enhancementType == "edge" }.sorted { $0.name < $1.name }

            if !team.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(team) { item in
                            TeamLevelCapsule(item: item)
                        }
                    }
                }
            }

            if !clv.isEmpty {
                CLVLevelLine(items: clv)
            }

            if !edge.isEmpty {
                VStack(spacing: 8) {
                    ForEach(edge) { item in
                        EdgeEnhancementRow(item: item)
                    }
                }
            }
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

    private func loadAll() async {
        async let mainLoad: () = load()
        async let weeksLoad: () = weeksStore.load(syndicateId: runner.syndicateId, leagueIds: nil)
        _ = await (mainLoad, weeksLoad)
        updateDefaultWeekIfNeeded()
    }

    private func updateDefaultWeekIfNeeded() {
        guard selectedWeekId == nil else { return }
        selectedWeekId = weeksStore.weeks
            .filter { week in completedBets.contains { $0.weekId == week.weekId } }
            .max { $0.weekStartDt < $1.weekStartDt }?
            .weekId
    }

    private func load() async {
        isLoading = true
        async let statsFetch = try? BettorService().fetchStats(bettorId: runner.bettorId)
        async let runnersFetch = try? RunnerService().fetchRunner(bettorId: runner.bettorId)
        async let syndicatesFetch = try? SyndicateService().fetchSyndicate(bettorId: runner.bettorId)
        async let leagueBalancesFetch = try? BettorService().fetchLeagueBalances(bettorId: runner.bettorId)
        async let leaguesFetch = try? LeagueService().fetchLeagues()
        async let betsFetch = try? TxnService().fetchCompletedBets(bettorId: runner.bettorId)
        async let enhancedFetch = try? EnhancementService().fetchEnhanced(bettorId: runner.bettorId, syndicateId: runner.syndicateId)
        stats = await statsFetch ?? nil
        syndicateRunners = await runnersFetch ?? []
        syndicates = await syndicatesFetch ?? []
        leagueBalances = await leagueBalancesFetch ?? []
        leagues = await leaguesFetch ?? []
        completedBets = await betsFetch ?? []
        syndicateEnhanced = await enhancedFetch ?? []
        isLoading = false
    }
}

#Preview("SheetRunner") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetRunner(runner: Mock.runnerAdmin)
            .environmentObject(AppTheme())
    }
}
