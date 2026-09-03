import SwiftUI

private enum RankFilterCategory: String, CaseIterable {
    case conf = "Conf"
    case color = "Color"
    case region = "Region"
    case category = "Category"
}

private enum OddsSortBy: String, CaseIterable {
    case ml = "ML"
    case spr = "SPR"
    case ou = "O/U"
}

private enum RankOddsMode {
    case rank, odds
}

struct ViewRank: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var selectedYear: Int
    @State private var mode: RankOddsMode = .rank
    @State private var sortBy: OddsSortBy = .ml
    @State private var selectedConf: String? = nil
    @State private var selectedColor: String? = nil
    @State private var selectedRegion: String? = nil
    @State private var selectedCategory: String? = nil
    @State private var selectedDiv: String? = nil
    @State private var expandedCategory: RankFilterCategory? = nil
    @State private var isYearExpanded = false
    @State private var seasons: [TeamSeason] = []
    @State private var records: [TeamOddsRecent] = []
    @State private var teams: [Team] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let seasonService = TeamSeasonService()
    private let oddsService = TeamOddsRecentService()
    private let teamService = TeamService()

    private struct TrendEntry: Identifiable {
        let record: TeamOddsRecent
        let season: TeamSeason?
        var id: Int { record.teamId }
    }

    init(league: League) {
        self.league = league
        let currentYear = Calendar.current.component(.year, from: .now)
        _selectedYear = State(initialValue: league.yrData ?? currentYear)
    }

    private var years: [Int] {
        let currentYear = Calendar.current.component(.year, from: .now)
        let end = max(league.yrData ?? currentYear, 2020)
        return Array(2020 ... end).reversed()
    }

    private var entries: [TrendEntry] {
        let seasonsByTeam = Dictionary(uniqueKeysWithValues: seasons.map { ($0.teamId, $0) })
        return records.map { TrendEntry(record: $0, season: seasonsByTeam[$0.teamId]) }
    }

    private var confs: [String] {
        mode == .rank
            ? Array(Set(seasons.compactMap(\.conf))).sorted()
            : Array(Set(records.compactMap(\.conf))).sorted()
    }
    private var colors: [String] {
        mode == .rank
            ? Array(Set(seasons.compactMap(\.color))).sorted()
            : Array(Set(records.compactMap(\.color))).sorted()
    }
    private var regions: [String] {
        mode == .rank
            ? Array(Set(seasons.compactMap(\.region))).sorted()
            : Array(Set(records.compactMap(\.region))).sorted()
    }
    private var categories: [String] {
        mode == .rank
            ? Array(Set(seasons.compactMap(\.category))).sorted()
            : Array(Set(records.compactMap(\.category))).sorted()
    }

    private var availableFilterCategories: [RankFilterCategory] {
        RankFilterCategory.allCases.filter { !filterOptions(for: $0).isEmpty }
    }

    private func filterOptions(for category: RankFilterCategory) -> [String] {
        switch category {
        case .conf:     return confs
        case .color:    return colors
        case .region:   return regions
        case .category: return categories
        }
    }

    private func categoryIcon(for category: RankFilterCategory) -> String {
        switch category {
        case .conf:     return "figure.and.child.holdinghands"
        case .color:    return "paint.bucket.classic"
        case .region:   return "mappin.and.ellipse"
        case .category: return "theatermasks.fill"
        }
    }

    private func filterCategoryValue(_ category: RankFilterCategory) -> String? {
        switch category {
        case .conf:     return selectedConf
        case .color:    return selectedColor
        case .region:   return selectedRegion
        case .category: return selectedCategory
        }
    }

    private func setCategoryValue(_ category: RankFilterCategory, _ value: String?) {
        switch category {
        case .conf:     selectedConf = value
        case .color:    selectedColor = value
        case .region:   selectedRegion = value
        case .category: selectedCategory = value
        }
    }

    /// Wipes a category's whole selection chain (including `selectedDiv` for `.conf`).
    private func clearCategory(_ category: RankFilterCategory) {
        setCategoryValue(category, nil)
        if category == .conf { selectedDiv = nil }
    }

    private func clearAllCategories() {
        for category in RankFilterCategory.allCases { clearCategory(category) }
    }

    /// Tapping the top-level category capsule (or its resolved breadcrumb) either drills in
    /// (clearing any other active category, so only one facet is ever active) or, if it's
    /// already the active one, wipes it and returns to showing all categories.
    private func tapCategory(_ category: RankFilterCategory) {
        if expandedCategory == category {
            clearCategory(category)
            expandedCategory = nil
        } else {
            clearAllCategories()
            expandedCategory = category
        }
    }

    private func tapValue(_ category: RankFilterCategory, _ value: String) {
        setCategoryValue(category, value)
        if category == .conf { selectedDiv = nil }
    }

    /// Bottom-up unselect: tapping the currently selected div again steps back up to the
    /// div options list instead of wiping the whole conf selection.
    private func tapDiv(_ value: String) {
        selectedDiv = (selectedDiv == value) ? nil : value
    }

    private static let cfbFBSDivOrder = ["ACC", "B10", "B12", "SEC", "IND"]

    private var divByTeamId: [Int: String] {
        Dictionary(uniqueKeysWithValues: teams.compactMap { t in t.div.map { (t.id, $0) } })
    }

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

    private var displayedSeasons: [TeamSeason] {
        seasons
            .filter { season in
                if let conf = selectedConf, season.conf != conf { return false }
                if let color = selectedColor, season.color != color { return false }
                if let region = selectedRegion, season.region != region { return false }
                if let category = selectedCategory, season.category != category { return false }
                if let div = selectedDiv, divByTeamId[season.teamId] != div { return false }
                return true
            }
            .sorted { $0.winPct > $1.winPct }
    }

    private var displayedEntries: [TrendEntry] {
        entries
            .filter { entry in
                if let conf = selectedConf, entry.record.conf != conf { return false }
                if let color = selectedColor, entry.record.color != color { return false }
                if let region = selectedRegion, entry.record.region != region { return false }
                if let category = selectedCategory, entry.record.category != category { return false }
                if let div = selectedDiv, divByTeamId[entry.record.teamId] != div { return false }
                return true
            }
            .sorted { lhs, rhs in
                switch sortBy {
                case .ml:  return (lhs.season?.winPct ?? 0) > (rhs.season?.winPct ?? 0)
                case .spr: return (lhs.record.atsCoverPct ?? 0) > (rhs.record.atsCoverPct ?? 0)
                case .ou:  return (lhs.record.overPct ?? 0) > (rhs.record.overPct ?? 0)
                }
            }
    }

    private func cycleIcon(for sort: OddsSortBy) -> String {
        switch sort {
        case .spr: return "arrow.left.and.line.vertical.and.arrow.right"
        case .ou:  return "arrow.up.and.line.horizontal.and.arrow.down"
        case .ml:  return "lines.measurement.vertical"
        }
    }

    private func advanceSortBy() {
        let order: [OddsSortBy] = [.ml, .spr, .ou]
        let idx = order.firstIndex(of: sortBy) ?? 0
        sortBy = order[(idx + 1) % order.count]
    }

    /// Collapsed to just the selected year until tapped; tapping a year in the expanded
    /// list re-collapses immediately.
    @ViewBuilder
    private var yearSegment: some View {
        if isYearExpanded {
            ForEach(years, id: \.self) { year in
                FilterChip(label: String(year), isSelected: selectedYear == year) {
                    selectedYear = year
                    isYearExpanded = false
                }
            }
        } else {
            FilterChip(label: String(selectedYear), isSelected: true) {
                isYearExpanded = true
            }
        }
    }

    /// Breadcrumb drill-down: at rest, shows all available categories; selecting one replaces
    /// its siblings with that category's values; Conf additionally drills one level deeper
    /// into divisions once a conf value is chosen.
    @ViewBuilder
    private var filterBreadcrumbSegment: some View {
        if let category = expandedCategory {
            FilterChip(label: filterCategoryValue(category) ?? "", isSelected: true, icon: categoryIcon(for: category)) {
                tapCategory(category)
            }
            if category == .conf {
                if let div = selectedDiv {
                    FilterChip(label: div, isSelected: true) {
                        tapDiv(div)
                    }
                } else if selectedConf != nil {
                    ForEach(divs, id: \.self) { div in
                        FilterChip(label: div, isSelected: false) {
                            tapDiv(div)
                        }
                    }
                } else {
                    ForEach(filterOptions(for: category), id: \.self) { value in
                        FilterChip(label: value, isSelected: false) {
                            tapValue(category, value)
                        }
                    }
                }
            } else if filterCategoryValue(category) == nil {
                ForEach(filterOptions(for: category), id: \.self) { value in
                    FilterChip(label: value, isSelected: false) {
                        tapValue(category, value)
                    }
                }
            }
        } else {
            ForEach(availableFilterCategories, id: \.self) { category in
                FilterChip(label: "", isSelected: false, icon: categoryIcon(for: category)) {
                    tapCategory(category)
                }
            }
        }
    }

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            VStack(spacing: 0) {
                // Year + filter breadcrumb — a single row, so drilling into a filter never
                // stacks additional rows below it.
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        if mode == .odds {
                            RowCapsuleButton(systemName: cycleIcon(for: sortBy), isSelected: true) {
                                advanceSortBy()
                            }
                        }
                        yearSegment
                        filterBreadcrumbSegment
                    }
                    .padding(.vertical, 8)
                }
                .padding(.horizontal)
                .padding(.vertical, 4)

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
                            Button("Retry") { Task { await fetchData() } }
                                .buttonStyle(.bordered)
                        }
                        .padding()
                        Spacer()
                    } else if mode == .rank {
                        if displayedSeasons.isEmpty {
                            Spacer()
                            Text("No standings available")
                                .foregroundStyle(.secondary)
                            Spacer()
                        } else {
                            ScrollView {
                                LazyVStack(spacing: 12) {
                                    ForEach(Array(displayedSeasons.enumerated()), id: \.element.id) { index, season in
                                        NavigationLink {
                                            ViewSchedLoader(teamId: season.teamId, leagueId: season.leagueId, initialYear: selectedYear)
                                        } label: {
                                            CardRank(rank: index + 1, season: season)
                                        }
                                        .buttonStyle(.plain)
                                    }
                                }
                                .padding(.horizontal)
                                .padding(.top, 12)
                            }
                        }
                    } else {
                        if displayedEntries.isEmpty {
                            Spacer()
                            Text("No odds trends available")
                                .foregroundStyle(.secondary)
                            Spacer()
                        } else {
                            ScrollView {
                                LazyVStack(spacing: 12) {
                                    ForEach(Array(displayedEntries.enumerated()), id: \.element.id) { index, entry in
                                        NavigationLink {
                                            ViewSchedLoader(teamId: entry.record.teamId, leagueId: entry.record.leagueId, initialYear: selectedYear)
                                        } label: {
                                            CardRank(rank: index + 1, season: entry.season, record: entry.record)
                                        }
                                        .buttonStyle(.plain)
                                    }
                                }
                                .padding(.horizontal)
                                .padding(.top, 12)
                            }
                        }
                    }
                }
            }
            .animation(.easeInOut(duration: 0.2), value: selectedConf)
            .animation(.easeInOut(duration: 0.2), value: selectedDiv)
            .animation(.easeInOut(duration: 0.2), value: expandedCategory)
            .animation(.easeInOut(duration: 0.2), value: isYearExpanded)
        }
        .navigationTitle("\(league.abbr) \(mode == .rank ? "Ranks" : "Odds")")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                SegmentedToggle(
                    leftLabel: "Rank",
                    rightLabel: "Odds",
                    isRightSelected: Binding(
                        get: { mode == .odds },
                        set: { mode = $0 ? .odds : .rank }
                    )
                )
            }
        }
        .task(id: selectedYear) { await fetchData() }
        .task(id: league.id) { await loadTeams() }
    }

    private func fetchData() async {
        isLoading = true
        errorMessage = nil
        do {
            async let seasonsTask = seasonService.fetchTeamSeasons(leagueId: league.id, yr: selectedYear)
            async let recordsTask = oddsService.fetchTeamOddsRecent(leagueId: league.id, yr: selectedYear)
            let (fetchedSeasons, fetchedRecords) = try await (seasonsTask, recordsTask)
            seasons = fetchedSeasons
            records = fetchedRecords
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }

    private func loadTeams() async {
        teams = TeamListCache.load(leagueId: league.id)
        if let fresh = try? await teamService.fetchTeams(leagueId: league.id) {
            teams = fresh
            TeamListCache.save(fresh, leagueId: league.id)
        }
    }
}

// MARK: - TeamListCache

private enum TeamListCache {
    private static func key(leagueId: Int) -> String { "cachedTeams_\(leagueId)" }

    static func load(leagueId: Int) -> [Team] {
        guard let data = UserDefaults.standard.data(forKey: key(leagueId: leagueId)),
              let saved = try? JSONDecoder().decode([Team].self, from: data)
        else { return [] }
        return saved
    }

    static func save(_ items: [Team], leagueId: Int) {
        guard let data = try? JSONEncoder().encode(items) else { return }
        UserDefaults.standard.set(data, forKey: key(leagueId: leagueId))
    }
}

#Preview("ViewRank") {
    NavigationStack {
        ViewRank(league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
