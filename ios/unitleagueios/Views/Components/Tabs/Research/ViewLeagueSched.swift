import SwiftUI

struct ViewLeagueSched: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var selectedDate: Date = .now
    @State private var games: [Game] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let gameService = GameService()

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            VStack(spacing: 0) {
                DateNavigationHeader(selectedDate: $selectedDate)

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
                                        CardGameSlim(game: game)
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
    }

    private func fetchGames() async {
        isLoading = true
        errorMessage = nil
        do {
            games = try await gameService.fetchGames(date: selectedDate, leagueId: league.id)
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }
}

#Preview("ViewLeagueSched") {
    NavigationStack {
        ViewLeagueSched(league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
