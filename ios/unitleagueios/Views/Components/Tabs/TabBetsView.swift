import SwiftUI

struct TabBetsView: View {
    @EnvironmentObject private var theme: AppTheme
    @EnvironmentObject private var betStore: BetStore
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId")            private var bettorId: Int            = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int  = 0
    @AppStorage("profileSymbol")       private var profileSymbol: String    = ProfileOption.symbols[0]
    @State private var selectedDate: Date = .now
    @State private var selectedLeagueId: Int? = nil
    @State private var selectedTeamId: Int? = nil
    @State private var selectedBetType: String = "ALL"
    @State private var selectedOddsType: String = "ALL"
    @State private var showJuiceOnly: Bool = false
    @State private var selectedBookmarkParlayLegs: [PlacedBet]?
    @State private var odds: [Odds] = []
    @State private var allOdds: [Odds] = []
    @State private var games: [Game] = []
    @State private var allGames: [Game] = []
    @State private var teams: [Team] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var selectedBet: SelectedBet?
    @State private var juiceTeamLevels: [Int: Int] = [:]

    @State private var txnRecords: [Txn] = []
    @State private var syndicates: [Int: Syndicate] = [:]
    @State private var myRunners: [Int: Runner] = [:]
    @State private var isLoadingActive = false
    @State private var isEditingActiveBets = false
    @State private var showEnhancedActive = true
    @State private var showEnhancedHistory = true
    @State private var completedRecords: [Txn] = []
    @State private var historyLegs: [Txn] = []
    @State private var historySyndicates: [Int: Syndicate] = [:]
    @State private var isLoadingHistory = false
    @State private var selectedHistorySyndicateId: Int? = nil
    @State private var selectedHistoryBetType: HistoryBetType? = nil
    @State private var selectedHistoryResult: Bool? = nil

    private let oddsTypeCycle = ["ALL", "ML", "SPR", "O/U"]

    private func cycleIcon(for label: String) -> String {
        switch label {
        case "SPR": return "arrow.left.and.line.vertical.and.arrow.right"
        case "O/U": return "arrow.up.and.line.horizontal.and.arrow.down"
        case "ALL": return "square.grid.3x2"
        default:    return "lines.measurement.vertical" // ML
        }
    }

    private func advanceOddsType() {
        let idx = oddsTypeCycle.firstIndex(of: selectedOddsType) ?? 0
        selectedOddsType = oddsTypeCycle[(idx + 1) % oddsTypeCycle.count]
    }

    /// The odds type Calendar's `CardGame` cards show — Calendar has no "ALL" concept,
    /// so it falls back to ML while the shared toggle sits idle.
    private var effectiveCalendarOddsType: String {
        selectedOddsType == "ALL" ? "ML" : selectedOddsType
    }

    private enum HistoryBetType: String, CaseIterable {
        case straight, parlay, unit
    }

    private let oddsService = OddsService()
    private let teamService = TeamService()
    private let enhancementService = EnhancementService()
    private let gameService = GameService()
    private let txnService = TxnService()
    private let syndicateService = SyndicateService()
    private let runnerService = RunnerService()

    private let leagues: [(label: String, id: Int)] = [
        ("NBA", 1), ("NFL", 2), ("NHL", 3),
        ("MLB", 4), ("CFB", 5), ("CBB", 6)
    ]

    private let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private let timeInputFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withColonSeparatorInTimeZone]
        return f
    }()

    private func parseGameTime(_ iso: String?) -> Date {
        guard let iso, let date = timeInputFormatter.date(from: iso) else { return .distantFuture }
        return date
    }

    private var oddsSectionTitle: String {
        switch selectedOddsType {
        case "ALL":   return "The Slate"
        case "ML":    return "Moneyline Bets"
        case "SPR":   return "Spread Bets"
        case "O/U":   return "Total Bets"
        default:      return ""
        }
    }

    private var dateKey: String { dateFormatter.string(from: selectedDate) }
    private var fetchKey: String { "\(dateKey)-\(selectedLeagueId ?? 0)" }
    private var leaguesWithOdds: Set<Int> { Set(allOdds.map(\.leagueId)) }
    private var leaguesWithGames: Set<Int> { Set(allGames.map(\.leagueId)) }

    private var filteredTeams: [Team] {
        let gameTeamIds = Set(odds.flatMap { [$0.homeTeamId, $0.awayTeamId] })
        return teams.filter { gameTeamIds.contains($0.id) }
    }

    private var availabilityTint: (Int) -> Color? {
        { id in
            selectedBetType == "Calendar"
                ? (leaguesWithGames.contains(id) ? theme.win : theme.loss)
                : (leaguesWithOdds.contains(id) ? theme.win : theme.loss)
        }
    }

    private var filteredOdds: [Odds] {
        let byType: [Odds]
        switch selectedOddsType {
        case "SPR": byType = odds.filter { $0.sprAwayPrice != nil && $0.sprHomePrice != nil }
        case "O/U": byType = odds.filter { $0.overPrice != nil && $0.underPrice != nil }
        default:    byType = odds // "ALL" and "ML"
        }
        let scoped = showJuiceOnly
            ? byType.filter { juiceTeamLevels[$0.homeTeamId] != nil || juiceTeamLevels[$0.awayTeamId] != nil }
            : byType
        let teamFiltered: [Odds]
        if let teamId = selectedTeamId {
            teamFiltered = scoped.filter { $0.homeTeamId == teamId || $0.awayTeamId == teamId }
        } else {
            teamFiltered = scoped
        }
        return teamFiltered.sorted { parseGameTime($0.gameTime) < parseGameTime($1.gameTime) }
    }

    private var oddsByGameId: [Int: Odds] {
        Dictionary(uniqueKeysWithValues: odds.map { ($0.gameId, $0) })
    }

    private var displayedGames: [Game] {
        let filtered: [Game]
        if let teamId = selectedTeamId {
            filtered = games.filter { $0.homeTeamId == teamId || $0.awayTeamId == teamId }
        } else {
            filtered = games
        }
        return filtered.sorted { parseGameTime($0.gameTime) < parseGameTime($1.gameTime) }
    }

    private var selectedTeamAbbr: String? {
        guard let selectedTeamId else { return nil }
        return teams.first(where: { $0.id == selectedTeamId })?.abbr
    }

    private func matchesLeagueTeamFilter(_ txn: Txn) -> Bool {
        (selectedLeagueId == nil || txn.leagueId == selectedLeagueId)
            && (selectedTeamAbbr == nil || txn.home == selectedTeamAbbr || txn.away == selectedTeamAbbr)
    }

    private func groupBySyndicate(_ txns: [Txn]) -> [(syndicateId: Int, singles: [Txn], parlays: [[Txn]])] {
        let bySyndicate = Dictionary(grouping: txns, by: \.syndicateId)
        return bySyndicate.keys.sorted().map { sid in
            let group = bySyndicate[sid] ?? []
            let singles = group.filter { $0.parlayId == nil }
                .sorted { parseGameTime($0.gameTime) < parseGameTime($1.gameTime) }
            let parlayMap = Dictionary(grouping: group.filter { $0.parlayId != nil }, by: { $0.parlayId! })
            let parlays = parlayMap.values.map { $0 }
                .sorted { parseGameTime($0.first?.gameTime) < parseGameTime($1.first?.gameTime) }
            return (syndicateId: sid, singles: singles, parlays: parlays)
        }
    }

    private var activeBets: [Txn] {
        txnRecords.filter { $0.canceled != true && matchesLeagueTeamFilter($0) }
    }

    private var activeDateSections: [(dateKey: String, groups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]])])] {
        let byDate = Dictionary(grouping: activeBets) { $0.gameDate ?? "" }
        return byDate.keys.sorted().map { dk in
            (dateKey: dk, groups: groupBySyndicate(byDate[dk] ?? []))
        }
    }

    private var historyRecords: [Txn] {
        completedRecords
            .filter { $0.txnType == "unit" || matchesLeagueTeamFilter($0) }
            .sorted { ($0.insertDt ?? "") > ($1.insertDt ?? "") }
    }

    private var historyGroups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]], units: [Txn])] {
        let bySyndicate = Dictionary(grouping: historyRecords, by: \.syndicateId)
        return bySyndicate.keys.sorted().compactMap { sid in
            let group = bySyndicate[sid] ?? []
            let singles = group.filter { $0.parlayId == nil && $0.txnType == "straight" }
                .sorted { ($0.insertDt ?? "") > ($1.insertDt ?? "") }
            let parlayIds = Set(group.filter { $0.parlayId != nil }.map { $0.parlayId! })
            let parlayMap = Dictionary(grouping: historyLegs.filter { $0.parlayId.map(parlayIds.contains) ?? false }, by: { $0.parlayId! })
            let parlays = parlayMap.values.map { $0 }
                .sorted { ($0.first?.insertDt ?? "") > ($1.first?.insertDt ?? "") }
            let units = group.filter { $0.txnType == "unit" }
                .sorted { ($0.insertDt ?? "") > ($1.insertDt ?? "") }
            guard !singles.isEmpty || !parlays.isEmpty || !units.isEmpty else { return nil }
            return (syndicateId: sid, singles: singles, parlays: parlays, units: units)
        }
    }

    private func parlayResult(_ legs: [Txn]) -> Bool? {
        if legs.contains(where: { $0.won == false }) { return false }
        if legs.contains(where: { $0.won == nil })    { return nil }
        return true
    }

    private var filteredHistoryGroups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]], units: [Txn])] {
        historyGroups.compactMap { group in
            if let sid = selectedHistorySyndicateId, group.syndicateId != sid {
                return nil
            }
            let showSingles = selectedHistoryBetType == nil || selectedHistoryBetType == .straight
            let showParlays = selectedHistoryBetType == nil || selectedHistoryBetType == .parlay
            let showUnits = selectedHistoryBetType == nil || selectedHistoryBetType == .unit

            let singles = showSingles ? group.singles.filter { txn in
                selectedHistoryResult == nil || txn.won == selectedHistoryResult
            } : []
            let parlays = showParlays ? group.parlays.filter { legs in
                selectedHistoryResult == nil || parlayResult(legs) == selectedHistoryResult
            } : []
            let units = (showUnits && selectedHistoryResult == nil) ? group.units : []

            guard !singles.isEmpty || !parlays.isEmpty || !units.isEmpty else { return nil }
            return (syndicateId: group.syndicateId, singles: singles, parlays: parlays, units: units)
        }
    }

    private struct HistoryDateEntry {
        let dateKey: String
        let syndicateId: Int
        var singles: [Txn] = []
        var parlays: [[Txn]] = []
    }

    private var historyDateSections: [(dateKey: String, groups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]])])] {
        var entries: [String: [Int: HistoryDateEntry]] = [:]
        for group in filteredHistoryGroups {
            for txn in group.singles {
                let dk = txn.gameDate ?? ""
                var entry = entries[dk]?[group.syndicateId] ?? HistoryDateEntry(dateKey: dk, syndicateId: group.syndicateId)
                entry.singles.append(txn)
                entries[dk, default: [:]][group.syndicateId] = entry
            }
            for legs in group.parlays {
                let dk = legs.first?.gameDate ?? ""
                var entry = entries[dk]?[group.syndicateId] ?? HistoryDateEntry(dateKey: dk, syndicateId: group.syndicateId)
                entry.parlays.append(legs)
                entries[dk, default: [:]][group.syndicateId] = entry
            }
        }
        return entries.keys.sorted(by: >).map { dk in
            let bySyndicate = entries[dk] ?? [:]
            let groups = bySyndicate.keys.sorted().map { sid -> (syndicateId: Int, singles: [Txn], parlays: [[Txn]]) in
                let e = bySyndicate[sid]!
                return (syndicateId: sid, singles: e.singles, parlays: e.parlays)
            }
            return (dateKey: dk, groups: groups)
        }
    }

    private let sectionDateInputFmt: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private let sectionDateOutputFmt: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "EEE, MMM d"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private func formattedSectionDate(_ raw: String) -> String {
        guard let d = sectionDateInputFmt.date(from: raw) else { return raw }
        return sectionDateOutputFmt.string(from: d)
    }

    private var bookmarkSingles: [PlacedBet] {
        betStore.bookmarks.filter { $0.parlayGroupId == nil }
            .sorted { parseGameTime($0.gameTime) < parseGameTime($1.gameTime) }
    }

    private var bookmarkParlayGroups: [(id: UUID, legs: [PlacedBet])] {
        let grouped = Dictionary(grouping: betStore.bookmarks.filter { $0.parlayGroupId != nil }) { $0.parlayGroupId! }
        return grouped.map { (id: $0.key, legs: $0.value) }
            .sorted { parseGameTime($0.legs.first?.gameTime) < parseGameTime($1.legs.first?.gameTime) }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                VStack(spacing: 0) {
                    DateNavigationHeader(selectedDate: $selectedDate)

                    // League filter
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(leagues, id: \.label) { league in
                                FilterChip(
                                    label: league.label,
                                    isSelected: selectedLeagueId == league.id,
                                    availabilityTint: availabilityTint(league.id)
                                ) {
                                    selectedLeagueId = (selectedLeagueId == league.id) ? nil : league.id
                                    selectedTeamId = nil
                                }
                            }
                        }
                        .padding(.horizontal)
                        .padding(.vertical, 8)
                    }

                    // Team filter — shown between league and bet types when a league is selected
                    if !filteredTeams.isEmpty {
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 8) {
                                ForEach(filteredTeams) { team in
                                    FilterChip(
                                        label: team.abbr,
                                        isSelected: selectedTeamId == team.id
                                    ) {
                                        selectedTeamId = (selectedTeamId == team.id) ? nil : team.id
                                    }
                                }
                            }
                            .padding(.horizontal)
                            .padding(.vertical, 8)
                        }
                        .transition(.opacity.combined(with: .move(edge: .top)))
                    }

                    // Capsule row: Calendar, ALL, Bookmarks, Active, History — widened to fill the row
                    HStack(spacing: 8) {
                        RowCapsuleButton(systemName: "calendar", isSelected: selectedBetType == "Calendar") {
                            selectedBetType = "Calendar"
                        }
                        .frame(maxWidth: .infinity)

                        RowCapsuleButton(systemName: "square.grid.3x2", isSelected: selectedBetType == "ALL") {
                            selectedBetType = "ALL"
                        }
                        .frame(maxWidth: .infinity)

                        RowCapsuleButton(
                            systemName: betStore.bookmarks.isEmpty ? "bookmark" : "bookmark.fill",
                            isSelected: selectedBetType == "Bookmarks"
                        ) {
                            selectedBetType = "Bookmarks"
                        }
                        .frame(maxWidth: .infinity)

                        RowCapsuleButton(systemName: "receipt.fill", isSelected: selectedBetType == "Active") {
                            selectedBetType = "Active"
                            Task { await fetchActiveBetsData() }
                        }
                        .frame(maxWidth: .infinity)

                        RowCapsuleButton(systemName: "bitcoinsign.bank.building.fill", isSelected: selectedBetType == "History") {
                            selectedBetType = "History"
                            Task { await loadHistoryData() }
                        }
                        .frame(maxWidth: .infinity)
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)

                    Divider().background(theme.divider(colorScheme))

                    // Content
                    Group {
                        switch selectedBetType {
                        case "Active":
                            activeBetsContent
                        case "History":
                            historyContent
                        case "Bookmarks":
                            bookmarksContent
                        default:
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
                                    Button("Retry") { Task { await fetchContent() } }
                                        .buttonStyle(.bordered)
                                }
                                .padding()
                                Spacer()
                            } else if selectedBetType == "Calendar" {
                                calendarContent
                            } else {
                                oddsContent
                            }
                        }
                    }
                }
                .animation(.easeInOut(duration: 0.2), value: teams.isEmpty)
                .sheet(item: $selectedBet) { bet in
                    SheetConfirmBet(bet: bet, bettorId: bettorId, syndicateId: selectedSyndicateId)
                }
                .sheet(isPresented: Binding(
                    get: { selectedBookmarkParlayLegs != nil },
                    set: { if !$0 { selectedBookmarkParlayLegs = nil } }
                )) {
                    if let legs = selectedBookmarkParlayLegs {
                        SheetConfirmParlay(currentBet: nil, bettorId: bettorId, syndicateId: selectedSyndicateId, savedLegs: legs)
                    }
                }
                .gesture(
                    DragGesture(minimumDistance: 40, coordinateSpace: .local)
                        .onEnded { value in
                            let horizontal = value.translation.width
                            let vertical = abs(value.translation.height)
                            guard abs(horizontal) > vertical else { return }
                            if horizontal < 0 {
                                selectedDate = Calendar.current.date(byAdding: .day, value: 1, to: selectedDate) ?? selectedDate
                            } else {
                                selectedDate = Calendar.current.date(byAdding: .day, value: -1, to: selectedDate) ?? selectedDate
                            }
                        }
                )
            }
            .tabToolbar()
        }
        .task(id: fetchKey) { await fetchContent() }
        .task(id: dateKey) { await fetchAllContent() }
        .task(id: selectedSyndicateId) { await loadJuiceTeams() }
        .task(id: bettorId) { await fetchActiveBetsData() }
        .task(id: bettorId) { await loadRunners() }
        .task(id: bettorId) { await loadHistoryData() }
        .onChange(of: selectedLeagueId) { _, leagueId in
            Task {
                if let id = leagueId {
                    await fetchTeams(leagueId: id)
                } else {
                    teams = []
                    selectedTeamId = nil
                }
            }
        }
    }

    // MARK: - Odds content (Slate: ALL/ML/SPR/O-U, with optional Juice filter)

    private var oddsContent: some View {
        Group {
            if filteredOdds.isEmpty {
                Spacer()
                Text("No odds available")
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 20) {
                        HStack {
                            Text(oddsSectionTitle)
                                .font(.title3.weight(.bold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            Spacer()
                            RowCapsuleButton(systemName: cycleIcon(for: selectedOddsType), isSelected: true) {
                                advanceOddsType()
                            }
                            RowCapsuleButton(systemName: "syringe.fill", isSelected: showJuiceOnly) {
                                showJuiceOnly.toggle()
                            }
                            Text("\(filteredOdds.count)")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.horizontal, 16)

                        LazyVStack(spacing: 12) {
                            ForEach(filteredOdds) { odd in
                                if selectedOddsType == "ALL" {
                                    ZStack {
                                        NavigationLink {
                                            ViewGameDetail(
                                                gameId: odd.gameId,
                                                home: odd.homeAbbr,
                                                away: odd.awayAbbr,
                                                homeTeamId: odd.homeTeamId,
                                                awayTeamId: odd.awayTeamId,
                                                leagueId: odd.leagueId
                                            )
                                        } label: { Color.clear }
                                        CardGameOdds(
                                            odd: odd,
                                            teamLevels: juiceTeamLevels,
                                            showJuiceCapsule: showJuiceOnly
                                        ) { bet in selectedBet = bet }
                                    }
                                } else {
                                    ZStack {
                                        NavigationLink {
                                            ViewGameDetail(
                                                gameId: odd.gameId,
                                                home: odd.homeAbbr,
                                                away: odd.awayAbbr,
                                                homeTeamId: odd.homeTeamId,
                                                awayTeamId: odd.awayTeamId,
                                                leagueId: odd.leagueId
                                            )
                                        } label: { Color.clear }
                                        CardGameOddSingle(odd: odd, betType: selectedOddsType) { bet in selectedBet = bet }
                                    }
                                }
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                    .padding(.top, 12)
                    .padding(.bottom, 24)
                }
            }
        }
    }

    // MARK: - Calendar content

    private var calendarContent: some View {
        Group {
            if displayedGames.isEmpty {
                Spacer()
                Text("No games scheduled")
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 20) {
                        HStack {
                            Text("Schedule")
                                .font(.title3.weight(.bold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            Spacer()
                            RowCapsuleButton(systemName: cycleIcon(for: effectiveCalendarOddsType), isSelected: true) {
                                advanceOddsType()
                            }
                            Text("\(displayedGames.count)")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.horizontal, 16)

                        LazyVStack(spacing: 12) {
                            ForEach(displayedGames) { game in
                                NavigationLink {
                                    ViewGameDetail(
                                        gameId: game.id,
                                        home: game.home,
                                        away: game.away,
                                        homeTeamId: game.homeTeamId,
                                        awayTeamId: game.awayTeamId,
                                        leagueId: game.leagueId
                                    )
                                } label: {
                                    CardGame(game: game, odds: oddsByGameId[game.id], oddsType: effectiveCalendarOddsType)
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                    .padding(.top, 12)
                    .padding(.bottom, 24)
                }
            }
        }
    }

    // MARK: - Active bets content

    private var activeBetsContent: some View {
        Group {
            if isLoadingActive {
                Spacer()
                ProgressView()
                Spacer()
            } else if activeBets.isEmpty {
                Spacer()
                Text("No active bets")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 20) {
                        HStack {
                            Text("Active Bets")
                                .font(.title3.weight(.bold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            Spacer()
                            Button(isEditingActiveBets ? "Done" : "Edit") {
                                isEditingActiveBets.toggle()
                            }
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                            .padding(.horizontal, 14)
                            .padding(.vertical, 6)
                            .background(theme.chipUnselected(colorScheme))
                            .clipShape(Capsule())
                            RowCapsuleButton(systemName: "bolt.batteryblock.fill", isSelected: showEnhancedActive, tint: theme.accent) {
                                showEnhancedActive.toggle()
                            }
                            Text("\(activeBets.count)")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.horizontal, 16)

                        ForEach(activeDateSections, id: \.dateKey) { section in
                            VStack(alignment: .leading, spacing: 10) {
                                Text(formattedSectionDate(section.dateKey))
                                    .font(.subheadline.weight(.bold))
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .padding(.horizontal, 16)

                                ForEach(section.groups, id: \.syndicateId) { group in
                                    VStack(alignment: .leading, spacing: 10) {
                                        SyndicateHeaderRow(
                                            syndicate: syndicates[group.syndicateId],
                                            syndicateId: group.syndicateId,
                                            runner: myRunners[group.syndicateId],
                                            bettorId: bettorId
                                        )

                                        ForEach(group.singles) { txn in
                                            CardPlacedBet(txn: txn, onCancel: { cancelBet(txn) }, isEditing: isEditingActiveBets, showEnhanced: showEnhancedActive)
                                        }

                                        ForEach(group.parlays, id: \.first?.parlayId) { legs in
                                            CardPlacedParlay(legs: legs, onCancel: { cancelParlay(legs) }, isEditing: isEditingActiveBets, showEnhanced: showEnhancedActive)
                                        }
                                    }
                                    .padding(.horizontal, 16)
                                }
                            }
                        }
                    }
                    .padding(.top, 12)
                    .padding(.bottom, 24)
                }
            }
        }
    }

    // MARK: - Bookmarks content

    private var bookmarksContent: some View {
        Group {
            if bookmarkSingles.isEmpty && bookmarkParlayGroups.isEmpty {
                Spacer()
                Text("No bookmarks")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 20) {
                        HStack {
                            Text("Bookmarked Bets")
                                .font(.title3.weight(.bold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            Spacer()
                            Text("\(bookmarkSingles.count + bookmarkParlayGroups.count)")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.horizontal, 16)

                        LazyVStack(spacing: 12) {
                            ForEach(bookmarkSingles) { bookmark in
                                CardBookmarkSingle(
                                    bookmark: bookmark,
                                    onTap: { selectedBet = SelectedBet(placedBet: bookmark) },
                                    onRemove: { betStore.removeBookmark(bookmark) }
                                )
                            }
                            ForEach(bookmarkParlayGroups, id: \.id) { group in
                                CardBookmarkParlay(
                                    legs: group.legs,
                                    onTap: { selectedBookmarkParlayLegs = group.legs },
                                    onRemove: { betStore.removeBookmarkParlay(groupId: group.id) }
                                )
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                    .padding(.top, 12)
                    .padding(.bottom, 24)
                }
            }
        }
    }

    // MARK: - History content

    private var historyContent: some View {
        Group {
            if isLoadingHistory {
                Spacer()
                ProgressView()
                Spacer()
            } else if historyRecords.isEmpty {
                Spacer()
                Text("No bet history")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 20) {
                        HStack(spacing: 8) {
                            Text("Bet History")
                                .font(.title3.weight(.bold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            Spacer()
                            historyFilterCapsules
                            RowCapsuleButton(systemName: "bolt.batteryblock.fill", isSelected: showEnhancedHistory, tint: theme.accent) {
                                showEnhancedHistory.toggle()
                            }
                            Text("\(filteredHistoryGroups.reduce(0) { $0 + $1.singles.count + $1.parlays.count + $1.units.count })")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.horizontal, 16)

                        if filteredHistoryGroups.isEmpty {
                            Text("No bets match these filters")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 16)
                        }

                        ForEach(historyDateSections, id: \.dateKey) { section in
                            VStack(alignment: .leading, spacing: 10) {
                                Text(formattedSectionDate(section.dateKey))
                                    .font(.subheadline.weight(.bold))
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .padding(.horizontal, 16)

                                ForEach(section.groups, id: \.syndicateId) { group in
                                    VStack(alignment: .leading, spacing: 10) {
                                        SyndicateHeaderRow(
                                            syndicate: historySyndicates[group.syndicateId],
                                            syndicateId: group.syndicateId,
                                            runner: myRunners[group.syndicateId],
                                            bettorId: bettorId
                                        )

                                        ForEach(group.singles) { txn in
                                            CardPlacedBet(txn: txn, onCancel: nil, showEnhanced: showEnhancedHistory)
                                        }

                                        ForEach(group.parlays, id: \.first?.parlayId) { legs in
                                            CardPlacedParlay(legs: legs, onCancel: nil, showEnhanced: showEnhancedHistory)
                                        }
                                    }
                                    .padding(.horizontal, 16)
                                }
                            }
                        }

                        let unitGroups = filteredHistoryGroups.filter { !$0.units.isEmpty }
                        if !unitGroups.isEmpty {
                            VStack(alignment: .leading, spacing: 10) {
                                Text("Account Activity")
                                    .font(.subheadline.weight(.bold))
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .padding(.horizontal, 16)

                                ForEach(unitGroups, id: \.syndicateId) { group in
                                    VStack(alignment: .leading, spacing: 10) {
                                        SyndicateHeaderRow(
                                            syndicate: historySyndicates[group.syndicateId],
                                            syndicateId: group.syndicateId,
                                            runner: myRunners[group.syndicateId],
                                            bettorId: bettorId
                                        )

                                        ForEach(group.units) { txn in
                                            CardUnits(txn: txn, syndicate: historySyndicates[group.syndicateId])
                                        }
                                    }
                                    .padding(.horizontal, 16)
                                }
                            }
                        }
                    }
                    .padding(.top, 12)
                    .padding(.bottom, 24)
                }
            }
        }
    }

    private var historySyndicateFilterOptions: [Int] {
        Array(Set(historyRecords.map(\.syndicateId))).sorted()
    }

    private func historySyndicateSymbol(_ sid: Int) -> String {
        historySyndicates[sid]?.symbol ?? "house.fill"
    }

    private func advanceHistorySyndicate() {
        let options = historySyndicateFilterOptions
        guard !options.isEmpty else { return }
        guard let current = selectedHistorySyndicateId, let idx = options.firstIndex(of: current) else {
            selectedHistorySyndicateId = options[0]
            return
        }
        let next = idx + 1
        selectedHistorySyndicateId = next < options.count ? options[next] : nil
    }

    private func advanceHistoryBetType() {
        let order: [HistoryBetType] = [.straight, .parlay, .unit]
        guard let current = selectedHistoryBetType, let idx = order.firstIndex(of: current) else {
            selectedHistoryBetType = order[0]
            return
        }
        let next = idx + 1
        selectedHistoryBetType = next < order.count ? order[next] : nil
    }

    private func advanceHistoryResult() {
        switch selectedHistoryResult {
        case .none:      selectedHistoryResult = true
        case .some(true): selectedHistoryResult = false
        case .some(false): selectedHistoryResult = nil
        }
    }

    private var historyRunnerCapsuleIcon: String {
        selectedHistorySyndicateId.map(historySyndicateSymbol) ?? "person.3.fill"
    }

    private var historyBetTypeCapsuleIcon: String {
        switch selectedHistoryBetType {
        case .straight: return "lock.square.fill"
        case .parlay:   return "lock.square.stack.fill"
        case .unit:     return "nairasign.circle.fill"
        case .none:     return "line.3.horizontal.decrease.circle"
        }
    }

    private var historyResultCapsuleIcon: String {
        switch selectedHistoryResult {
        case .some(true):  return "checkmark.circle.fill"
        case .some(false): return "x.circle.fill"
        case .none:        return "checkmark.circle"
        }
    }

    private var historyResultCapsuleTint: Color? {
        switch selectedHistoryResult {
        case .some(true):  return theme.win
        case .some(false): return theme.loss
        case .none:        return nil
        }
    }

    @ViewBuilder
    private var historyFilterCapsules: some View {
        if historySyndicateFilterOptions.count > 1 {
            RowCapsuleButton(systemName: historyRunnerCapsuleIcon, isSelected: selectedHistorySyndicateId != nil) {
                advanceHistorySyndicate()
            }
        }
        RowCapsuleButton(systemName: historyBetTypeCapsuleIcon, isSelected: selectedHistoryBetType != nil) {
            advanceHistoryBetType()
        }
        RowCapsuleButton(systemName: historyResultCapsuleIcon, isSelected: selectedHistoryResult != nil, tint: historyResultCapsuleTint) {
            advanceHistoryResult()
        }
    }

    private func fetchContent() async {
        isLoading = true
        errorMessage = nil
        games = []
        odds = []
        do {
            async let fetchedGames = gameService.fetchGames(date: selectedDate, leagueId: selectedLeagueId)
            async let fetchedOdds = oddsService.fetchOddBest(gameDt: dateKey, leagueId: selectedLeagueId)
            games = try await fetchedGames
            odds = try await fetchedOdds
        } catch {
            errorMessage = error.localizedDescription
        }
        isLoading = false
    }

    private func fetchAllContent() async {
        async let fetchedGames = gameService.fetchGames(date: selectedDate, leagueId: nil)
        async let fetchedOdds = oddsService.fetchOddBest(gameDt: dateKey, leagueId: nil)
        allGames = (try? await fetchedGames) ?? []
        allOdds = (try? await fetchedOdds) ?? []
    }

    private func fetchTeams(leagueId: Int) async {
        do {
            teams = try await teamService.fetchTeams(leagueId: leagueId)
        } catch {
            teams = []
        }
    }

    private func loadJuiceTeams() async {
        guard bettorId != 0 else { juiceTeamLevels = [:]; return }
        let enhanced = (try? await enhancementService.fetchEnhanced(bettorId: bettorId, syndicateId: selectedSyndicateId)) ?? []
        juiceTeamLevels = Dictionary(
            enhanced.filter { $0.enhancementType == "team" }.map { ($0.teamId, $0.level) },
            uniquingKeysWith: { first, _ in first }
        )
    }

    private var soloSyndicate: Syndicate {
        Syndicate(
            syndicateId: 0,
            name: "Solo Loser",
            isPublic: false,
            createdByBettorId: bettorId,
            symbol: profileSymbol,
            color: theme.accentOption.rawValue
        )
    }

    private func fetchActiveBetsData() async {
        guard bettorId != 0 else { return }
        isLoadingActive = txnRecords.isEmpty
        defer { isLoadingActive = false }
        txnRecords = (try? await txnService.fetchActiveBets(bettorId: bettorId)) ?? []
        let ids = Set(txnRecords.map(\.syndicateId))
        for sid in ids where syndicates[sid] == nil {
            if sid == 0 {
                syndicates[0] = soloSyndicate
                continue
            }
            if let result = try? await syndicateService.fetchSyndicate(syndicateId: sid, bettorId: nil) {
                syndicates[sid] = result.first
            }
        }
    }

    private func loadRunners() async {
        guard bettorId != 0 else { return }
        let fetched = (try? await runnerService.fetchRunner(bettorId: bettorId)) ?? []
        var map: [Int: Runner] = [:]
        for runner in fetched where runner.bettorId == bettorId {
            map[runner.syndicateId] = runner
        }
        myRunners = map
    }

    private func cancelBet(_ txn: Txn) {
        Task {
            try? await txnService.cancelTxn(txnId: txn.txnId)
            txnRecords.removeAll { $0.txnId == txn.txnId }
        }
    }

    private func cancelParlay(_ legs: [Txn]) {
        guard let txnId = legs.first?.txnId, let parlayId = legs.first?.parlayId else { return }
        Task {
            try? await txnService.cancelTxn(txnId: txnId)
            txnRecords.removeAll { $0.parlayId == parlayId }
        }
    }

    private func loadHistoryData() async {
        guard bettorId != 0 else { return }
        isLoadingHistory = completedRecords.isEmpty
        defer { isLoadingHistory = false }
        completedRecords = (try? await txnService.fetchCompletedBets(bettorId: bettorId)) ?? []
        historyLegs = (try? await txnService.fetchTxnBets(bettorId: bettorId)) ?? []
        let ids = Set(completedRecords.map(\.syndicateId))
        for sid in ids where historySyndicates[sid] == nil {
            if sid == 0 {
                historySyndicates[0] = soloSyndicate
                continue
            }
            if let result = try? await syndicateService.fetchSyndicate(syndicateId: sid, bettorId: nil) {
                historySyndicates[sid] = result.first
            }
        }
    }
}

#Preview {
    TabBetsView()
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
}
