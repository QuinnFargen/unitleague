import SwiftUI

struct ViewGameDetail: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let gameId: Int
    let home: String
    let away: String
    let homeTeamId: Int
    let awayTeamId: Int
    let leagueId: Int

    @AppStorage("bettorId")            private var bettorId: Int           = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int = 0

    @State private var odd: Odds?
    @State private var homeTeam: Team?
    @State private var awayTeam: Team?
    @State private var league: League?
    @State private var selectedBet: SelectedBet?
    @State private var oddMany: [OddMany] = []
    @State private var teamLevels: [Int: Int] = [:]
    @State private var gameBets: [Txn] = []
    @State private var completedGameBets: [Txn] = []
    @State private var runners: [Runner] = []
    @State private var showEnhanced = true

    private let oddService = OddsService()
    private let teamService = TeamService()
    private let leagueService = LeagueService()
    private let oddManyService = OddManyService()
    private let enhancementService = EnhancementService()
    private let txnService = TxnService()

    // Standard init — data loaded via .task { fetchData() }
    init(gameId: Int, home: String, away: String,
         homeTeamId: Int, awayTeamId: Int, leagueId: Int) {
        self.gameId = gameId
        self.home = home
        self.away = away
        self.homeTeamId = homeTeamId
        self.awayTeamId = awayTeamId
        self.leagueId = leagueId
    }

    // Preview/testing init — injects mock data, skips network fetch
    init(gameId: Int, home: String, away: String,
         homeTeamId: Int, awayTeamId: Int, leagueId: Int,
         preloadedOdd: Odds?,
         preloadedHomeTeam: Team?,
         preloadedAwayTeam: Team?,
         preloadedLeague: League?,
         preloadedOddMany: [OddMany]) {
        self.gameId = gameId
        self.home = home
        self.away = away
        self.homeTeamId = homeTeamId
        self.awayTeamId = awayTeamId
        self.leagueId = leagueId
        self._odd = State(initialValue: preloadedOdd)
        self._homeTeam = State(initialValue: preloadedHomeTeam)
        self._awayTeam = State(initialValue: preloadedAwayTeam)
        self._league = State(initialValue: preloadedLeague)
        self._oddMany = State(initialValue: preloadedOddMany)
    }

    private var isUpcoming: Bool {
        let gameDt = odd?.gameDt ?? oddMany.first?.gameDt
        guard let raw = gameDt else { return false }
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        guard let date = f.date(from: raw) else { return false }
        return Calendar.current.startOfDay(for: date) >= Calendar.current.startOfDay(for: Date())
    }

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            ScrollView {
                VStack(spacing: 16) {
                    if let odd {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Best Odds")
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 4)
                            CardGameOdds(odd: odd) { bet in selectedBet = bet }
                        }
                        .padding(.horizontal)
                    }

                    VStack(spacing: 12) {
                        if let awayTeam, let league {
                            NavigationLink {
                                ViewSched(team: awayTeam, league: league)
                            } label: {
                                CardTeam(team: awayTeam, league: league, showChevron: true, level: teamLevels[awayTeam.id])
                            }
                            .buttonStyle(.plain)
                        }

                        if let homeTeam, let league {
                            NavigationLink {
                                ViewSched(team: homeTeam, league: league)
                            } label: {
                                CardTeam(team: homeTeam, league: league, showChevron: true, level: teamLevels[homeTeam.id])
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal)

                    if !gameBets.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            HStack {
                                Text("Active Bets")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(.secondary)
                                Spacer()
                                RowCapsuleButton(systemName: "bolt.batteryblock.fill", isSelected: showEnhanced, tint: theme.accent) {
                                    showEnhanced.toggle()
                                }
                            }
                            .padding(.horizontal, 4)

                            VStack(spacing: 8) {
                                ForEach(gameBets) { txn in
                                    CardBetSlim(
                                        txn: txn,
                                        runner: runners.first(where: { $0.bettorId == txn.bettorId && $0.syndicateId == txn.syndicateId }),
                                        showEnhanced: showEnhanced
                                    )
                                }
                            }
                        }
                        .padding(.horizontal)
                    }

                    if !completedGameBets.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            HStack {
                                Text("Completed Bets")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(.secondary)
                                Spacer()
                                RowCapsuleButton(systemName: "bolt.batteryblock.fill", isSelected: showEnhanced, tint: theme.accent) {
                                    showEnhanced.toggle()
                                }
                            }
                            .padding(.horizontal, 4)

                            VStack(spacing: 8) {
                                ForEach(completedGameBets) { txn in
                                    CardBetSlim(
                                        txn: txn,
                                        runner: runners.first(where: { $0.bettorId == txn.bettorId && $0.syndicateId == txn.syndicateId }),
                                        showEnhanced: showEnhanced
                                    )
                                }
                            }
                        }
                        .padding(.horizontal)
                    }

                    if isUpcoming && !oddMany.isEmpty {
                        CardOddMany(odds: oddMany, awayAbbr: away, homeAbbr: home) { bet in
                            selectedBet = bet
                        }
                        .padding(.horizontal)
                    }
                }
                .padding(.top, 16)
                .padding(.bottom, 24)
            }
        }
        .navigationTitle("\(away) @ \(home)")
        .navigationBarTitleDisplayMode(.inline)
        .task { await fetchData() }
        .task { await loadTeamLevels() }
        .task { await loadBetsAndRunners() }
        .sheet(item: $selectedBet) { bet in
            SheetConfirmBet(bet: bet, bettorId: bettorId, syndicateId: selectedSyndicateId)
        }
    }

    private func fetchData() async {
        // Skip if data was preloaded (e.g. in previews)
        guard odd == nil else { return }
        async let oddsTask = try? oddService.fetchOddBest(gameId: gameId)
        async let teamsTask = try? teamService.fetchTeams(leagueId: leagueId)
        async let leaguesTask = try? leagueService.fetchLeagues()
        async let oddManyTask = try? oddManyService.fetchOddAll(gameId: gameId)

        let (odds, teams, leagues, many) = await (oddsTask, teamsTask, leaguesTask, oddManyTask)

        odd = odds?.first
        awayTeam = teams?.first { $0.id == awayTeamId }
        homeTeam = teams?.first { $0.id == homeTeamId }
        league = leagues?.first { $0.id == leagueId }
        oddMany = many ?? []
    }

    private func loadBetsAndRunners() async {
        async let activeTask = txnService.fetchActiveBets(gameId: gameId)
        async let completedTask = txnService.fetchCompletedBets(gameId: gameId, bettorId: bettorId)
        gameBets = (try? await activeTask) ?? []
        completedGameBets = (try? await completedTask) ?? []

        let syndicateIds = Set(gameBets.map(\.syndicateId)).union(completedGameBets.map(\.syndicateId))
        guard !syndicateIds.isEmpty else { return }
        runners = await withTaskGroup(of: [Runner].self) { group in
            for sid in syndicateIds {
                group.addTask { (try? await RunnerService().fetchRunner(syndicateId: sid)) ?? [] }
            }
            var all: [Runner] = []
            for await r in group { all.append(contentsOf: r) }
            return all
        }
    }

    private func loadTeamLevels() async {
        guard bettorId != 0, selectedSyndicateId != 0 else { return }
        let enhanced = (try? await enhancementService.fetchEnhanced(bettorId: bettorId, syndicateId: selectedSyndicateId)) ?? []
        var levels: [Int: Int] = [:]
        for item in enhanced where item.enhancementType == "team" {
            levels[item.teamId] = item.level
        }
        teamLevels = levels
    }
}

// MARK: - ViewGameDetailLoader

/// Resolves a game's odds by id then pushes into `ViewGameDetail`. Used where only
/// a `gameId` is on hand (e.g. from a `Txn`) and the home/away team ids and league
/// id `ViewGameDetail`'s init needs haven't been fetched yet.
struct ViewGameDetailLoader: View {
    let gameId: Int

    @State private var odd: Odds?

    private let oddService = OddsService()

    var body: some View {
        Group {
            if let odd {
                ViewGameDetail(
                    gameId: odd.gameId,
                    home: odd.homeAbbr,
                    away: odd.awayAbbr,
                    homeTeamId: odd.homeTeamId,
                    awayTeamId: odd.awayTeamId,
                    leagueId: odd.leagueId
                )
            } else {
                ProgressView().task { await load() }
            }
        }
    }

    private func load() async {
        odd = (try? await oddService.fetchOddBest(gameId: gameId))?.first
    }
}

#Preview("ViewGameDetail") {
    NavigationStack {
        ViewGameDetail(
            gameId: 101,
            home: "LAL",
            away: "BOS",
            homeTeamId: 1,
            awayTeamId: 2,
            leagueId: 1,
            preloadedOdd: Mock.odds,
            preloadedHomeTeam: Mock.teamLAL,
            preloadedAwayTeam: Mock.teamBOS,
            preloadedLeague: Mock.leagueNBA,
            preloadedOddMany: Mock.oddMany
        )
    }
    .environmentObject(AppTheme())
    .environmentObject(BetStore())
}
