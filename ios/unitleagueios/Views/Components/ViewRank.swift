import SwiftUI

struct ViewRank: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let league: League

    @State private var selectedYear: Int
    @State private var selectedConf: String? = nil
    @State private var selectedColor: String? = nil
    @State private var selectedRegion: String? = nil
    @State private var selectedCategory: String? = nil
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

                // Conf filter
                if !confs.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(confs, id: \.self) { conf in
                                FilterChip(label: conf, isSelected: selectedConf == conf) {
                                    selectedConf = (selectedConf == conf) ? nil : conf
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }
                }

                // Color filter
                if !colors.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(colors, id: \.self) { color in
                                FilterChip(label: color, isSelected: selectedColor == color) {
                                    selectedColor = (selectedColor == color) ? nil : color
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }
                }

                // Region filter
                if !regions.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(regions, id: \.self) { region in
                                FilterChip(label: region, isSelected: selectedRegion == region) {
                                    selectedRegion = (selectedRegion == region) ? nil : region
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }
                }

                // Category filter
                if !categories.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(categories, id: \.self) { category in
                                FilterChip(label: category, isSelected: selectedCategory == category) {
                                    selectedCategory = (selectedCategory == category) ? nil : category
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }
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
                                        ViewSchedLoader(teamId: season.teamId, leagueId: season.leagueId)
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
