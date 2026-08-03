import SwiftUI

struct ViewOdds: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var records: [TeamOddsRecent] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let service = TeamOddsRecentService()

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

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
                    Button("Retry") { Task { await fetchRecords() } }
                        .buttonStyle(.bordered)
                }
                .padding()
            } else if records.isEmpty {
                Text("No odds trends available")
                    .foregroundStyle(.secondary)
            } else {
                ScrollView {
                    LazyVStack(spacing: 12) {
                        ForEach(records) { record in
                            NavigationLink {
                                ViewSchedLoader(teamId: record.teamId, leagueId: record.leagueId)
                            } label: {
                                CardOddsTrend(record: record)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal)
                    .padding(.top, 12)
                }
            }
        }
        .navigationTitle("\(league.abbr) Odds")
        .navigationBarTitleDisplayMode(.inline)
        .task { await fetchRecords() }
    }

    private func fetchRecords() async {
        isLoading = true
        errorMessage = nil
        do {
            records = try await service.fetchTeamOddsRecent(leagueId: league.id)
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }
}

#Preview("ViewOdds") {
    NavigationStack {
        ViewOdds(league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
