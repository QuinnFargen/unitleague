import SwiftUI

struct TabResearchView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @State private var leagues: [League] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var expandedLeagueId: Int? = 1

    private let service = LeagueService()

    private let statusOrder = ["playoffs", "regular season", "preseason", "offseason"]
    private let statusLabels: [String: String] = [
        "playoffs": "Playoffs",
        "regular season": "Regular Season",
        "preseason": "Preseason",
        "offseason": "Offseason"
    ]

    private var groupedLeagues: [(status: String, leagues: [League])] {
        let byStatus = Dictionary(grouping: leagues, by: \.status)
        return statusOrder.compactMap { status in
            guard let group = byStatus[status], !group.isEmpty else { return nil }
            return (status: status, leagues: group)
        }
    }

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
                            LazyVStack(alignment: .leading, spacing: 20) {
                                ForEach(groupedLeagues, id: \.status) { group in
                                    VStack(alignment: .leading, spacing: 8) {
                                        Text(statusLabels[group.status] ?? group.status.capitalized)
                                            .font(.caption.weight(.semibold))
                                            .foregroundStyle(.secondary)
                                            .padding(.horizontal, 4)

                                        VStack(spacing: 12) {
                                            ForEach(group.leagues) { league in
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
