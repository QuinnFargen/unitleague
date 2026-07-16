import SwiftUI

struct TabResearchView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @State private var leagues: [League] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var expandedLeagueId: Int? = 1

    private let service = LeagueService()

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                Group {
                    if isLoading {
                        ProgressView()
                    } else if let error = errorMessage {
                        VStack(spacing: 12) {
                            Image(systemName: "exclamationmark.triangle")
                                .font(.largeTitle)
                                .foregroundStyle(theme.error)
                            Text(error)
                                .foregroundStyle(.secondary)
                                .multilineTextAlignment(.center)
                            Button("Retry") { fetchLeagues() }
                                .buttonStyle(.bordered)
                        }
                        .padding()
                    } else {
                        ScrollView {
                            LazyVStack(spacing: 12) {
                                ForEach(leagues) { league in
                                    CardLeague(
                                        league: league,
                                        isExpanded: expandedLeagueId == league.id
                                    ) {
                                        withAnimation(.easeInOut(duration: 0.2)) {
                                            expandedLeagueId = expandedLeagueId == league.id ? nil : league.id
                                        }
                                    }
                                }
                            }
                            .padding(.horizontal)
                            .padding(.top, 12)
                        }
                    }
                }
            }
            .tabToolbar()
        }
        .task { fetchLeagues() }
    }

    private func fetchLeagues() {
        isLoading = true
        errorMessage = nil
        Task {
            do {
                leagues = try await service.fetchLeagues()
            } catch {
                errorMessage = error.localizedDescription
            }
            isLoading = false
        }
    }
}

#Preview {
    TabResearchView()
        .environmentObject(AppTheme())
}
