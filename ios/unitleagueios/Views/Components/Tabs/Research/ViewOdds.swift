import SwiftUI

private enum OddsFilterCategory: String, CaseIterable {
    case conf = "Conf"
    case color = "Color"
    case region = "Region"
    case category = "Category"
}

struct ViewOdds: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var records: [TeamOddsRecent] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var selectedConf: String? = nil
    @State private var selectedColor: String? = nil
    @State private var selectedRegion: String? = nil
    @State private var selectedCategory: String? = nil
    @State private var expandedCategory: OddsFilterCategory? = nil

    private let service = TeamOddsRecentService()

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

    private var displayedRecords: [TeamOddsRecent] {
        records.filter { record in
            if let conf = selectedConf, record.conf != conf { return false }
            if let color = selectedColor, record.color != color { return false }
            if let region = selectedRegion, record.region != region { return false }
            if let category = selectedCategory, record.category != category { return false }
            return true
        }
    }

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            VStack(spacing: 0) {
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

                if !availableFilterCategories.isEmpty {
                    Divider()
                        .background(theme.divider(colorScheme))
                }

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
                    } else if displayedRecords.isEmpty {
                        Spacer()
                        Text("No odds trends available")
                            .foregroundStyle(.secondary)
                        Spacer()
                    } else {
                        ScrollView {
                            LazyVStack(spacing: 12) {
                                ForEach(displayedRecords) { record in
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
