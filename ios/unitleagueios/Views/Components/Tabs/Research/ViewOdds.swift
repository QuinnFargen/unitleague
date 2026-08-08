import SwiftUI

private enum OddsFilterCategory: String, CaseIterable {
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

struct ViewOdds: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var selectedYear: Int
    @State private var sortBy: OddsSortBy = .ml
    @State private var records: [TeamOddsRecent] = []
    @State private var seasons: [TeamSeason] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var selectedConf: String? = nil
    @State private var selectedColor: String? = nil
    @State private var selectedRegion: String? = nil
    @State private var selectedCategory: String? = nil
    @State private var expandedCategory: OddsFilterCategory? = nil

    private let service = TeamOddsRecentService()
    private let seasonService = TeamSeasonService()

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

    private var confs: [String]      { Array(Set(records.compactMap(\.conf))).sorted() }
    private var colors: [String]     { Array(Set(records.compactMap(\.color))).sorted() }
    private var regions: [String]    { Array(Set(records.compactMap(\.region))).sorted() }
    private var categories: [String] { Array(Set(records.compactMap(\.category))).sorted() }

    private var availableFilterCategories: [OddsFilterCategory] {
        OddsFilterCategory.allCases.filter { !filterOptions(for: $0).isEmpty }
    }

    private func filterOptions(for category: OddsFilterCategory) -> [String] {
        switch category {
        case .conf:     return confs
        case .color:    return colors
        case .region:   return regions
        case .category: return categories
        }
    }

    private func filterCategoryValue(_ category: OddsFilterCategory) -> String? {
        switch category {
        case .conf:     return selectedConf
        case .color:    return selectedColor
        case .region:   return selectedRegion
        case .category: return selectedCategory
        }
    }

    private func toggleFilterValue(_ category: OddsFilterCategory, _ value: String) {
        switch category {
        case .conf:     selectedConf     = (selectedConf == value)     ? nil : value
        case .color:    selectedColor    = (selectedColor == value)    ? nil : value
        case .region:   selectedRegion   = (selectedRegion == value)   ? nil : value
        case .category: selectedCategory = (selectedCategory == value) ? nil : value
        }
    }

    private var displayedEntries: [TrendEntry] {
        entries
            .filter { entry in
                if let conf = selectedConf, entry.record.conf != conf { return false }
                if let color = selectedColor, entry.record.color != color { return false }
                if let region = selectedRegion, entry.record.region != region { return false }
                if let category = selectedCategory, entry.record.category != category { return false }
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

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            VStack(spacing: 0) {
                // Year row
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(years, id: \.self) { year in
                            FilterChip(label: String(year), isSelected: selectedYear == year) {
                                selectedYear = year
                            }
                        }
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                }

                // Sort row
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(OddsSortBy.allCases, id: \.self) { option in
                            FilterChip(label: option.rawValue, isSelected: sortBy == option) {
                                sortBy = option
                            }
                        }
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                }

                // Filter category row
                if !availableFilterCategories.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(availableFilterCategories, id: \.self) { category in
                                FilterChip(
                                    label: category.rawValue,
                                    isSelected: expandedCategory == category || filterCategoryValue(category) != nil
                                ) {
                                    expandedCategory = (expandedCategory == category) ? nil : category
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }
                }

                // Options row for the expanded filter category
                if let category = expandedCategory {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(filterOptions(for: category), id: \.self) { value in
                                FilterChip(label: value, isSelected: filterCategoryValue(category) == value) {
                                    toggleFilterValue(category, value)
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
                            Button("Retry") { Task { await fetchRecords() } }
                                .buttonStyle(.bordered)
                        }
                        .padding()
                        Spacer()
                    } else if displayedEntries.isEmpty {
                        Spacer()
                        Text("No odds trends available")
                            .foregroundStyle(.secondary)
                        Spacer()
                    } else {
                        ScrollView {
                            LazyVStack(spacing: 12) {
                                ForEach(displayedEntries) { entry in
                                    NavigationLink {
                                        ViewSchedLoader(teamId: entry.record.teamId, leagueId: entry.record.leagueId, initialYear: selectedYear)
                                    } label: {
                                        CardOddsTrend(record: entry.record, season: entry.season)
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
        .navigationTitle("\(league.abbr) Odds")
        .navigationBarTitleDisplayMode(.inline)
        .task(id: selectedYear) { await fetchRecords() }
    }

    private func fetchRecords() async {
        isLoading = true
        errorMessage = nil
        do {
            async let recordsTask = service.fetchTeamOddsRecent(leagueId: league.id, yr: selectedYear)
            async let seasonsTask = seasonService.fetchTeamSeasons(leagueId: league.id, yr: selectedYear)
            let (fetchedRecords, fetchedSeasons) = try await (recordsTask, seasonsTask)
            records = fetchedRecords
            seasons = fetchedSeasons
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
