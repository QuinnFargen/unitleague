import SwiftUI

struct TabResearchView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @State private var leagues: [League] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let service = LeagueService()

    private let statusOrder = ["playoffs", "regular season", "preseason", "offseason"]
    private let statusLabels: [String: String] = [
        "playoffs": "Playoffs",
        "regular season": "Regular Season",
        "preseason": "Preseason",
        "offseason": "Offseason"
    ]

    private let seasonStartDateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private func sortedForDisplay(_ group: [League], status: String) -> [League] {
        guard status == "offseason" else { return group }
        return group.sorted { lhs, rhs in
            let lhsDate = lhs.seasonStartDt.flatMap(seasonStartDateFormatter.date(from:))
            let rhsDate = rhs.seasonStartDt.flatMap(seasonStartDateFormatter.date(from:))
            switch (lhsDate, rhsDate) {
            case let (l?, r?): return l > r
            case (nil, _?):    return false
            case (_?, nil):    return true
            case (nil, nil):   return false
            }
        }
    }

    private var groupedLeagues: [(status: String, leagues: [League])] {
        let byStatus = Dictionary(grouping: leagues, by: \.status)
        return statusOrder.compactMap { status in
            guard let group = byStatus[status], !group.isEmpty else { return nil }
            return (status: status, leagues: sortedForDisplay(group, status: status))
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
                                                CardLeague(league: league)
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
