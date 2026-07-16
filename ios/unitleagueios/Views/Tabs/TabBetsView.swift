import SwiftUI

struct TabBetsView: View {
    @EnvironmentObject private var theme: AppTheme
    @EnvironmentObject private var betStore: BetStore
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId")            private var bettorId: Int            = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int  = 0
    @State private var selectedDate: Date = .now
    @State private var showingBookmarks = false
    @State private var selectedLeagueId: Int? = nil
    @State private var selectedTeamId: Int? = nil
    @State private var selectedBetType: String = "None"
    @State private var odds: [Odds] = []
    @State private var allOdds: [Odds] = []
    @State private var games: [Game] = []
    @State private var allGames: [Game] = []
    @State private var teams: [Team] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var selectedBet: SelectedBet?

    private let oddsService = OddsService()
    private let gameService = GameService()
    private let teamService = TeamService()

    private let leagues: [(label: String, id: Int)] = [
        ("NBA", 1), ("NFL", 2), ("NHL", 3),
        ("MLB", 4), ("CFB", 5), ("CBB", 6)
    ]

    private let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private var dateKey: String { dateFormatter.string(from: selectedDate) }
    private var fetchKey: String { "\(dateKey)-\(selectedLeagueId ?? 0)-\(selectedBetType)" }
    private var leaguesWithOdds: Set<Int> { Set(allOdds.map(\.leagueId)) }
    private var leaguesWithGames: Set<Int> { Set(allGames.map(\.leagueId)) }

    private var filteredTeams: [Team] {
        let gameTeamIds = Set(games.flatMap { [$0.homeTeamId, $0.awayTeamId] })
        return teams.filter { gameTeamIds.contains($0.id) }
    }

    private var availabilityTint: (Int) -> Color? {
        { id in
            if selectedBetType == "None" {
                return leaguesWithGames.contains(id) ? theme.win : theme.loss
            } else {
                return leaguesWithOdds.contains(id) ? theme.win : theme.loss
            }
        }
    }

    private var filteredOdds: [Odds] {
        let byType: [Odds]
        switch selectedBetType {
        case "SPR": byType = odds.filter { $0.sprAwayPrice != nil && $0.sprHomePrice != nil }
        case "O/U": byType = odds.filter { $0.overPrice != nil && $0.underPrice != nil }
        default:    byType = odds
        }
        guard let teamId = selectedTeamId else { return byType }
        return byType.filter { $0.homeTeamId == teamId || $0.awayTeamId == teamId }
    }

    private var displayedGames: [Game] {
        guard let teamId = selectedTeamId else { return games }
        return games.filter { $0.homeTeamId == teamId || $0.awayTeamId == teamId }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                VStack(spacing: 0) {
                    DateNavigationHeader(selectedDate: $selectedDate)

                    // League filter
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(leagues, id: \.label) { league in
                                FilterChip(
                                    label: league.label,
                                    isSelected: selectedLeagueId == league.id,
                                    availabilityTint: availabilityTint(league.id)
                                ) {
                                    selectedLeagueId = (selectedLeagueId == league.id) ? nil : league.id
                                    selectedTeamId = nil
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }

                    // Team filter — shown between league and bet types when a league is selected
                    if !filteredTeams.isEmpty {
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 8) {
                                ForEach(filteredTeams) { team in
                                    FilterChip(
                                        label: team.abbr,
                                        isSelected: selectedTeamId == team.id
                                    ) {
                                        selectedTeamId = (selectedTeamId == team.id) ? nil : team.id
                                    }
                                }
                            }
                            .padding(.horizontal)
                            .padding(.vertical, 8)
                        }
                        .transition(.opacity.combined(with: .move(edge: .top)))
                    }

                    // Bet type filter + bookmark
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(["None", "ALL", "ML", "SPR", "O/U"], id: \.self) { betType in
                                FilterChip(
                                    label: betType,
                                    isSelected: selectedBetType == betType
                                ) {
                                    selectedBetType = betType
                                }
                            }

                            Button {
                                showingBookmarks = true
                            } label: {
                                HStack(spacing: 5) {
                                    Image(systemName: "bookmark.fill")
                                    if !betStore.bookmarks.isEmpty {
                                        Text("\(betStore.bookmarks.count)")
                                            .font(.caption2.weight(.bold))
                                    }
                                }
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(betStore.bookmarks.isEmpty ? theme.primaryText(colorScheme) : theme.accent)
                                .padding(.horizontal, 14)
                                .padding(.vertical, 6)
                                .background(betStore.bookmarks.isEmpty ? theme.chipUnselected(colorScheme) : theme.accent.opacity(0.15))
                                .clipShape(Capsule())
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }

                    Divider().background(theme.divider(colorScheme))

                    // Content
                    Group {
                        if isLoading {
                            Spacer()
                            ProgressView()
                            Spacer()
                        } else if let error = errorMessage {
                            Spacer()
                            VStack(spacing: 12) {
                                Image(systemName: "exclamationmark.triangle")
                                    .font(.largeTitle)
                                    .foregroundStyle(theme.error)
                                Text(error)
                                    .foregroundStyle(.secondary)
                                    .multilineTextAlignment(.center)
                                Button("Retry") { Task { await fetchContent() } }
                                    .buttonStyle(.bordered)
                            }
                            .padding()
                            Spacer()
                        } else if selectedBetType == "None" {
                            if displayedGames.isEmpty {
                                Spacer()
                                Text("No games scheduled")
                                    .foregroundStyle(.secondary)
                                Spacer()
                            } else {
                                ScrollView {
                                    LazyVStack(spacing: 12) {
                                        ForEach(displayedGames) { game in
                                            NavigationLink {
                                                ViewGameDetail(
                                                    gameId: game.id,
                                                    home: game.home,
                                                    away: game.away,
                                                    homeTeamId: game.homeTeamId,
                                                    awayTeamId: game.awayTeamId,
                                                    leagueId: game.leagueId
                                                )
                                            } label: {
                                                CardGame(game: game)
                                            }
                                            .buttonStyle(.plain)
                                        }
                                    }
                                    .padding(.horizontal)
                                    .padding(.top, 12)
                                }
                            }
                        } else {
                            if filteredOdds.isEmpty {
                                Spacer()
                                Text("No odds available")
                                    .foregroundStyle(.secondary)
                                Spacer()
                            } else {
                                ScrollView {
                                    LazyVStack(spacing: 12) {
                                        ForEach(filteredOdds) { odd in
                                            if selectedBetType == "ALL" {
                                                ZStack {
                                                    NavigationLink {
                                                        ViewGameDetail(
                                                            gameId: odd.gameId,
                                                            home: odd.homeAbbr,
                                                            away: odd.awayAbbr,
                                                            homeTeamId: odd.homeTeamId,
                                                            awayTeamId: odd.awayTeamId,
                                                            leagueId: odd.leagueId
                                                        )
                                                    } label: { Color.clear }
                                                    CardGameOdds(odd: odd) { bet in selectedBet = bet }
                                                }
                                            } else {
                                                NavigationLink {
                                                    ViewGameDetail(
                                                        gameId: odd.gameId,
                                                        home: odd.homeAbbr,
                                                        away: odd.awayAbbr,
                                                        homeTeamId: odd.homeTeamId,
                                                        awayTeamId: odd.awayTeamId,
                                                        leagueId: odd.leagueId
                                                    )
                                                } label: {
                                                    CardOddSingle(odd: odd, betType: selectedBetType)
                                                }
                                                .buttonStyle(.plain)
                                            }
                                        }
                                    }
                                    .padding(.horizontal)
                                    .padding(.top, 12)
                                }
                            }
                        }
                    }
                }
                .animation(.easeInOut(duration: 0.2), value: teams.isEmpty)
                .sheet(isPresented: $showingBookmarks) {
                    SheetBookmarks()
                }
                .sheet(item: $selectedBet) { bet in
                    SheetConfirmBet(bet: bet, bettorId: bettorId, syndicateId: selectedSyndicateId)
                }
                .gesture(
                    DragGesture(minimumDistance: 40, coordinateSpace: .local)
                        .onEnded { value in
                            let horizontal = value.translation.width
                            let vertical = abs(value.translation.height)
                            guard abs(horizontal) > vertical else { return }
                            if horizontal < 0 {
                                selectedDate = Calendar.current.date(byAdding: .day, value: 1, to: selectedDate) ?? selectedDate
                            } else {
                                selectedDate = Calendar.current.date(byAdding: .day, value: -1, to: selectedDate) ?? selectedDate
                            }
                        }
                )
            }
            .tabToolbar()
        }
        .task(id: fetchKey) { await fetchContent() }
        .task(id: dateKey) { await fetchAllContent() }
        .onChange(of: selectedLeagueId) { _, leagueId in
            Task {
                if let id = leagueId {
                    await fetchTeams(leagueId: id)
                } else {
                    teams = []
                    selectedTeamId = nil
                }
            }
        }
    }

    private func fetchContent() async {
        isLoading = true
        errorMessage = nil
        games = []
        odds = []
        do {
            async let fetchedGames = gameService.fetchGames(date: selectedDate, leagueId: selectedLeagueId)
            async let fetchedOdds = oddsService.fetchOddBest(gameDt: dateKey, leagueId: selectedLeagueId)
            games = try await fetchedGames
            odds = try await fetchedOdds
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }

    private func fetchAllContent() async {
        async let fetchedGames = gameService.fetchGames(date: selectedDate, leagueId: nil)
        async let fetchedOdds = oddsService.fetchOddBest(gameDt: dateKey, leagueId: nil)
        allGames = (try? await fetchedGames) ?? []
        allOdds = (try? await fetchedOdds) ?? []
    }

    private func fetchTeams(leagueId: Int) async {
        do {
            teams = try await teamService.fetchTeams(leagueId: leagueId)
        } catch {
            teams = []
        }
    }
}

#Preview {
    TabBetsView()
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
}
