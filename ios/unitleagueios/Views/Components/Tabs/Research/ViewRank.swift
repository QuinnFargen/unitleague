import SwiftUI

private enum RankFilterCategory: String, CaseIterable {
    case conf = "Conf"
    case color = "Color"
    case region = "Region"
    case category = "Category"
}

struct ViewRank: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var selectedYear: Int
    @State private var selectedConf: String? = nil
    @State private var selectedColor: String? = nil
    @State private var selectedRegion: String? = nil
    @State private var selectedCategory: String? = nil
    @State private var expandedCategory: RankFilterCategory? = nil
    @State private var seasons: [TeamSeason] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let service = TeamSeasonService()

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

    private var confs: [String]      { Array(Set(seasons.compactMap(\.conf))).sorted() }
    private var colors: [String]     { Array(Set(seasons.compactMap(\.color))).sorted() }
    private var regions: [String]    { Array(Set(seasons.compactMap(\.region))).sorted() }
    private var categories: [String] { Array(Set(seasons.compactMap(\.category))).sorted() }

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

    private func filterCategoryValue(_ category: RankFilterCategory) -> String? {
        switch category {
        case .conf:     return selectedConf
        case .color:    return selectedColor
        case .region:   return selectedRegion
        case .category: return selectedCategory
        }
    }

    private func toggleFilterValue(_ category: RankFilterCategory, _ value: String) {
        switch category {
        case .conf:     selectedConf     = (selectedConf == value)     ? nil : value
        case .color:    selectedColor    = (selectedColor == value)    ? nil : value
        case .region:   selectedRegion   = (selectedRegion == value)   ? nil : value
        case .category: selectedCategory = (selectedCategory == value) ? nil : value
        }
    }

    private var displayedSeasons: [TeamSeason] {
        seasons
            .filter { season in
                if let conf = selectedConf, season.conf != conf { return false }
                if let color = selectedColor, season.color != color { return false }
                if let region = selectedRegion, season.region != region { return false }
                if let category = selectedCategory, season.category != category { return false }
                return true
            }
            .sorted { $0.winPct > $1.winPct }
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
                            Button("Retry") { Task { await fetchSeasons() } }
                                .buttonStyle(.bordered)
                        }
                        .padding()
                        Spacer()
                    } else if displayedSeasons.isEmpty {
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
                }
            }
        }
        .navigationTitle("\(league.abbr) Ranks")
        .navigationBarTitleDisplayMode(.inline)
        .task(id: selectedYear) { await fetchSeasons() }
    }

    private func fetchSeasons() async {
        isLoading = true
        errorMessage = nil
        do {
            seasons = try await service.fetchTeamSeasons(leagueId: league.id, yr: selectedYear)
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }
}

#Preview("ViewRank") {
    NavigationStack {
        ViewRank(league: Mock.leagueNBA)
    }
    .environmentObject(AppTheme())
}
