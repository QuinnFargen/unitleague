import SwiftUI

struct ViewLeagueSched: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var selectedDate: Date
    @State private var games: [Game] = []
    @State private var odds: [Odds] = []
    @State private var gameDates: [LeagueGameDate] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let gameService = GameService()
    private let oddsService = OddsService()
    private let gameDateService = LeagueGameDateService()

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    init(league: League) {
        self.league = league
        if league.status == "offseason",
           let raw = league.lastSchedDt,
           let parsed = Self.dateFormatter.date(from: raw) {
            _selectedDate = State(initialValue: parsed)
        } else {
            _selectedDate = State(initialValue: .now)
        }
    }

    private var oddsByGameId: [Int: Odds] {
        Dictionary(uniqueKeysWithValues: odds.map { ($0.gameId, $0) })
    }

    private var validDateSet: Set<Date> {
        Set(gameDates.compactMap { Self.dateFormatter.date(from: $0.gameDt) })
    }

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            VStack(spacing: 0) {
                DateNavigationHeader(selectedDate: $selectedDate, validDates: validDateSet.isEmpty ? nil : validDateSet)

                Divider()
                    .background(theme.divider(colorScheme))

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
                            Button("Retry") { Task { await fetchGames() } }
                                .buttonStyle(.bordered)
                        }
                        .padding()
                        Spacer()
                    } else if games.isEmpty {
                        Spacer()
                        Text("No games on this date")
                            .foregroundStyle(.secondary)
                        Spacer()
                    } else {
                        ScrollView {
                            LazyVStack(spacing: 8) {
                                ForEach(games) { game in
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
                                        CardGame(game: game, odds: oddsByGameId[game.id])
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                            .padding(.horizontal)
                            .padding(.top, 8)
                        }
                    }
                }
            }
        }
        .navigationTitle("\(league.abbr) Sched")
        .navigationBarTitleDisplayMode(.inline)
        .task(id: selectedDate) { await fetchGames() }
        .task(id: league.id) { await loadGameDates() }
    }

    private func fetchGames() async {
        isLoading = true
        errorMessage = nil
        do {
            async let fetchedGames = gameService.fetchGames(date: selectedDate, leagueId: league.id)
            async let fetchedOdds = oddsService.fetchOddBest(gameDt: Self.dateFormatter.string(from: selectedDate), leagueId: league.id)
            games = try await fetchedGames
            odds = (try? await fetchedOdds) ?? []
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }

    private func loadGameDates() async {
        gameDates = GameDatesCache.load(leagueId: league.id)
        if let fresh = try? await gameDateService.fetchGameDates(leagueId: league.id) {
            gameDates = GameDatesCache.merge(fresh, leagueId: league.id)
        }
    }
}

// MARK: - GameDatesCache

private enum GameDatesCache {
    private static func key(leagueId: Int) -> String { "cachedGameDates_\(leagueId)" }

    static func load(leagueId: Int) -> [LeagueGameDate] {
        guard let data = UserDefaults.standard.data(forKey: key(leagueId: leagueId)),
              let saved = try? JSONDecoder().decode([LeagueGameDate].self, from: data)
        else { return [] }
        return saved
    }

    static func merge(_ fresh: [LeagueGameDate], leagueId: Int) -> [LeagueGameDate] {
        let existing = load(leagueId: leagueId)
        var byDate = Dictionary(uniqueKeysWithValues: existing.map { ($0.gameDt, $0) })
        for item in fresh { byDate[item.gameDt] = item }
        let merged = Array(byDate.values).sorted { $0.gameDt < $1.gameDt }
        save(merged, leagueId: leagueId)
        return merged
    }

    private static func save(_ items: [LeagueGameDate], leagueId: Int) {
        guard let data = try? JSONEncoder().encode(items) else { return }
        UserDefaults.standard.set(data, forKey: key(leagueId: leagueId))
    }
}

#Preview("ViewLeagueSched") {
    NavigationStack {
        ViewLeagueSched(league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
