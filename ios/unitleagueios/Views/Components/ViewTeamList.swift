import SwiftUI

struct ViewTeamList: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var teams: [Team] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var selectedConf: String? = nil
    @State private var selectedDiv: String? = nil

    private let teamService = TeamService()

    private var confs: [String] {
        Array(Set(teams.filter { $0.id != 50000 && $0.id != 60000 }.compactMap(\.conf))).sorted()
    }

    private static let cfbFBSDivOrder = ["ACC", "B10", "B12", "SEC", "IND"]

    private var divs: [String] {
        guard let conf = selectedConf else { return [] }
        let raw = Array(Set(teams.filter { $0.id != 50000 && $0.id != 60000 && $0.conf == conf }.compactMap(\.div)))
        if league.id == 5 && conf == "FBS" {
            let priority = Self.cfbFBSDivOrder
            return raw.sorted {
                let li = priority.firstIndex(of: $0) ?? Int.max
                let ri = priority.firstIndex(of: $1) ?? Int.max
                return li == ri ? $0 < $1 : li < ri
            }
        }
        return raw.sorted()
    }

    private var displayedTeams: [Team] {
        teams.filter { team in
            if team.id == 50000 || team.id == 60000 { return false }
            if let conf = selectedConf, team.conf != conf { return false }
            if let div = selectedDiv, team.div != div { return false }
            return true
        }
    }

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
                    Button("Retry") { Task { await fetchTeams() } }
                        .buttonStyle(.bordered)
                }
                .padding()
            } else {
                VStack(spacing: 0) {
                    // Conf filter
                    if !confs.isEmpty {
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 8) {
                                ForEach(confs, id: \.self) { conf in
                                    FilterChip(
                                        label: conf,
                                        isSelected: selectedConf == conf
                                    ) {
                                        if selectedConf == conf {
                                            selectedConf = nil
                                            selectedDiv = nil
                                        } else {
                                            selectedConf = conf
                                            selectedDiv = nil
                                        }
                                    }
                                }
                            }
                            .padding(.horizontal)
                            .padding(.vertical, 8)
                        }
                    }

                    // Div filter — only shown when a conf is selected and divs exist
                    if !divs.isEmpty {
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 8) {
                                ForEach(divs, id: \.self) { div in
                                    FilterChip(
                                        label: div,
                                        isSelected: selectedDiv == div
                                    ) {
                                        selectedDiv = (selectedDiv == div) ? nil : div
                                    }
                                }
                            }
                            .padding(.horizontal)
                            .padding(.vertical, 8)
                        }
                        .transition(.opacity.combined(with: .move(edge: .top)))
                    }

                    if !confs.isEmpty {
                        Divider()
                            .background(theme.divider(colorScheme))
                    }

                    ScrollView {
                        LazyVStack(spacing: 12) {
                            ForEach(displayedTeams) { team in
                                NavigationLink {
                                    ViewSched(team: team, league: league)
                                } label: {
                                    CardTeam(team: team, league: league, showChevron: true)
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.horizontal)
                        .padding(.top, 8)
                    }
                }
                .animation(.easeInOut(duration: 0.2), value: selectedConf)
            }
        }
        .navigationTitle(league.abbr)
        .navigationBarTitleDisplayMode(.inline)
        .task { await fetchTeams() }
    }

    private func fetchTeams() async {
        isLoading = true
        errorMessage = nil
        do {
            teams = try await teamService.fetchTeams(leagueId: league.id)
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }
}

#Preview("ViewTeamList") {
    NavigationStack {
        ViewTeamList(league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
