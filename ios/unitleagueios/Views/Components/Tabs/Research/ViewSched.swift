import SwiftUI

private enum SchedMode: String, CaseIterable {
    case year = "Year"
    case recent = "Recent"
}

private enum SchedOppCategory: String, CaseIterable {
    case conf = "Conf"
    case color = "Color"
    case region = "Region"
    case mascot = "Mascot"
}

struct ViewSched: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let team: Team
    let league: League

    @AppStorage("bettorId") private var bettorId: Int = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int = 0

    @State private var selectedMode: SchedMode = .year
    @State private var selectedYear: Int = Calendar.current.component(.year, from: .now)
    @State private var selectedOppConf: String? = nil
    @State private var selectedOppColor: String? = nil
    @State private var selectedOppRegion: String? = nil
    @State private var selectedOppMascot: String? = nil
    @State private var expandedOppCategory: SchedOppCategory? = nil
    @State private var leagueTeams: [Team] = []
    @State private var schedule: [Sched] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var scrollTarget: String? = nil
    @State private var teamLevel: Int? = nil

    private var lastFinalId: String? {
        schedule.first { $0.teamScore != nil && $0.oppScore != nil }?.id
    }

    private let schedService = SchedService()
    private let enhancementService = EnhancementService()
    private let teamService = TeamService()

    init(team: Team, league: League) {
        self.team = team
        self.league = league
        let currentYear = Calendar.current.component(.year, from: .now)
        _selectedYear = State(initialValue: league.yrData ?? currentYear)
    }

    private var years: [Int] {
        let currentYear = Calendar.current.component(.year, from: .now)
        let end = max(league.yrData ?? currentYear, 2020)
        return Array(2020 ... end).reversed()
    }

    private var opponentPool: [Team] {
        leagueTeams.filter { $0.id != 50000 && $0.id != 60000 && $0.id != team.id }
    }

    private var oppConfs: [String]   { Array(Set(opponentPool.compactMap(\.conf))).sorted() }
    private var oppColors: [String]  { Array(Set(opponentPool.compactMap(\.color))).sorted() }
    private var oppRegions: [String] { Array(Set(opponentPool.compactMap(\.region))).sorted() }
    private var oppMascots: [String] { Array(Set(opponentPool.compactMap(\.mascot))).sorted() }

    private var availableOppCategories: [SchedOppCategory] {
        SchedOppCategory.allCases.filter { !oppOptions(for: $0).isEmpty }
    }

    private func oppOptions(for category: SchedOppCategory) -> [String] {
        switch category {
        case .conf:   return oppConfs
        case .color:  return oppColors
        case .region: return oppRegions
        case .mascot: return oppMascots
        }
    }

    private func oppCategoryValue(_ category: SchedOppCategory) -> String? {
        switch category {
        case .conf:   return selectedOppConf
        case .color:  return selectedOppColor
        case .region: return selectedOppRegion
        case .mascot: return selectedOppMascot
        }
    }

    private func toggleOppValue(_ category: SchedOppCategory, _ value: String) {
        switch category {
        case .conf:   selectedOppConf   = (selectedOppConf == value)   ? nil : value
        case .color:  selectedOppColor  = (selectedOppColor == value)  ? nil : value
        case .region: selectedOppRegion = (selectedOppRegion == value) ? nil : value
        case .mascot: selectedOppMascot = (selectedOppMascot == value) ? nil : value
        }
    }

    private var fetchKey: String {
        "\(selectedMode)-\(selectedYear)-\(selectedOppConf ?? "")-\(selectedOppColor ?? "")-\(selectedOppRegion ?? "")-\(selectedOppMascot ?? "")"
    }

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            VStack(spacing: 0) {
                CardTeam(team: team, league: league, level: teamLevel)
                    .padding(.horizontal)
                    .padding(.top, 8)

                // Mode picker
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(SchedMode.allCases, id: \.self) { mode in
                            FilterChip(label: mode.rawValue, isSelected: selectedMode == mode) {
                                selectedMode = mode
                            }
                        }
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                }

                // Secondary filter row — years, or opponent filter categories
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        if selectedMode == .year {
                            ForEach(years, id: \.self) { year in
                                FilterChip(label: String(year), isSelected: selectedYear == year) {
                                    selectedYear = year
                                }
                            }
                        } else {
                            ForEach(availableOppCategories, id: \.self) { category in
                                FilterChip(
                                    label: category.rawValue,
                                    isSelected: expandedOppCategory == category || oppCategoryValue(category) != nil
                                ) {
                                    expandedOppCategory = (expandedOppCategory == category) ? nil : category
                                }
                            }
                        }
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                }
                .animation(.easeInOut(duration: 0.2), value: selectedMode)

                // Tertiary row — options for the expanded opponent filter category
                if selectedMode == .recent, let category = expandedOppCategory {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(oppOptions(for: category), id: \.self) { value in
                                FilterChip(label: value, isSelected: oppCategoryValue(category) == value) {
                                    toggleOppValue(category, value)
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }
                    .transition(.opacity.combined(with: .move(edge: .top)))
                }

                Divider()
                    .background(theme.divider(colorScheme))

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
                            Button("Retry") { Task { await fetchSchedule() } }
                                .buttonStyle(.bordered)
                        }
                        .padding()
                        Spacer()
                    } else if schedule.isEmpty {
                        Spacer()
                        Text("No schedule available")
                            .foregroundStyle(.secondary)
                        Spacer()
                    } else {
                        ScrollViewReader { proxy in
                            ScrollView {
                                LazyVStack(spacing: 12) {
                                    ForEach(schedule) { entry in
                                        CardSched(entry: entry, isHighlighted: entry.id == lastFinalId)
                                            .id(entry.id)
                                    }
                                }
                                .padding(.horizontal)
                                .padding(.top, 12)
                            }
                            .onChange(of: scrollTarget) { _, target in
                                guard let id = target else { return }
                                withAnimation { proxy.scrollTo(id, anchor: .top) }
                            }
                        }
                    }
                }
            }
        }
        .navigationTitle(team.name)
        .navigationBarTitleDisplayMode(.inline)
        .task(id: fetchKey) { await fetchSchedule() }
        .task { await loadTeamLevel() }
        .task { await loadLeagueTeams() }
    }

    private func loadLeagueTeams() async {
        leagueTeams = (try? await teamService.fetchTeams(leagueId: league.id)) ?? []
    }

    private func loadTeamLevel() async {
        guard bettorId != 0, selectedSyndicateId != 0 else { return }
        let enhanced = (try? await enhancementService.fetchEnhanced(bettorId: bettorId, syndicateId: selectedSyndicateId)) ?? []
        teamLevel = enhanced.first { $0.enhancementType == "team" && $0.teamId == team.id }?.level
    }

    private func fetchSchedule() async {
        isLoading = true
        errorMessage = nil
        schedule = []
        scrollTarget = nil
        do {
            let raw: [Sched]
            if selectedMode == .year {
                raw = try await schedService.fetchSchedule(teamId: team.id, yr: selectedYear)
            } else {
                raw = try await schedService.fetchSchedule(
                    teamId: team.id,
                    oppConf: selectedOppConf,
                    oppColor: selectedOppColor,
                    oppRegion: selectedOppRegion,
                    oppMascot: selectedOppMascot,
                    limit: 10
                )
            }
            schedule = raw.sorted { $0.gameNum > $1.gameNum }
            scrollTarget = lastFinalId
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }
}

// MARK: - ViewSchedLoader

/// Resolves a team/league by id then pushes into `ViewSched`. Used where only
/// ids are on hand (e.g. from `Odds`/`Enhanced`) and no full `Team`/`League`
/// has been fetched yet.
struct ViewSchedLoader: View {
    let teamId: Int
    let leagueId: Int

    @State private var team: Team?
    @State private var league: League?

    private let teamService = TeamService()
    private let leagueService = LeagueService()

    var body: some View {
        Group {
            if let team, let league {
                ViewSched(team: team, league: league)
            } else {
                ProgressView().task { await load() }
            }
        }
    }

    private func load() async {
        async let teamsTask = try? teamService.fetchTeams(leagueId: leagueId)
        async let leaguesTask = try? leagueService.fetchLeagues()
        let (teams, leagues) = await (teamsTask, leaguesTask)
        team = teams?.first { $0.id == teamId }
        league = leagues?.first { $0.id == leagueId }
    }
}


#Preview("ViewSched") {
    NavigationStack {
        ViewSched(team: Mock.teamLAL, league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
