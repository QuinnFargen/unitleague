import SwiftUI

struct TabJuiceView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId") private var bettorId: Int = 0

    @State private var txnRecords: [Txn] = []
    @State private var completedRecords: [Txn] = []
    @State private var syndicates: [Int: Syndicate] = [:]
    @State private var isLoading = false
    @State private var segment: BetSegment = .active

    // Juice state
    @State private var juiceSyndicates: [Syndicate] = []
    @State private var juiceSyndicateId: Int = 0
    @State private var availableOptions: [EnhanceOption] = []
    @State private var myEnhanced: [Enhanced] = []
    @State private var syndicateEnhanced: [Enhanced] = []
    @State private var pendingOption: EnhanceOption? = nil
    @State private var teamPickerTeams: [Team] = []
    @State private var isLoadingJuice = false
    @State private var isLoadingTeams = false
    @State private var showTeamPicker = false
    @State private var confirmOption: EnhanceOption? = nil

    private let txnService = TxnService()
    private let syndicateService = SyndicateService()
    private let enhancementService = EnhancementService()
    private let teamService = TeamService()

    private enum BetSegment: String, CaseIterable {
        case active = "Active"
        case history = "History"
        case juice = "Juice"
    }

    private var activeBets: [Txn] {
        txnRecords.filter { $0.canceled != true }
    }

    private var displayBets: [Txn] {
        segment == .active ? activeBets : completedRecords
    }

    private var syndicateGroups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]])] {
        let bySyndicate = Dictionary(grouping: displayBets, by: \.syndicateId)
        return bySyndicate.keys.sorted().map { sid in
            let group = bySyndicate[sid] ?? []
            let singles = group.filter { $0.parlayId == nil }
            let parlayMap = Dictionary(grouping: group.filter { $0.parlayId != nil }, by: { $0.parlayId! })
            let parlays = parlayMap.values.map { $0 }
            return (syndicateId: sid, singles: singles, parlays: parlays)
        }
    }

    // Deduplicated options by optionHash
    private var uniqueOptions: [EnhanceOption] {
        var seen = Set<String>()
        return availableOptions.filter { seen.insert($0.optionHash).inserted }
    }

    private var clvOptions: [EnhanceOption]  { uniqueOptions.filter { $0.enhancementType == "clv" } }
    private var teamOptions: [EnhanceOption] { uniqueOptions.filter { $0.enhancementType == "team" } }
    private var edgeOptions: [EnhanceOption] { uniqueOptions.filter { $0.enhancementType == "edge" } }

    private var chosenOptionHashes: Set<String> { Set(myEnhanced.map(\.optionHash)) }

    private func resolvedName(for enhanced: Enhanced) -> String {
        availableOptions.first { $0.enhancementId == enhanced.enhancementId }?.name ?? "Enhancement \(enhanced.enhancementId)"
    }

    private func resolvedType(for enhanced: Enhanced) -> String {
        availableOptions.first { $0.enhancementId == enhanced.enhancementId }?.enhancementType ?? "clv"
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                } else {
                    ScrollView {
                        VStack(spacing: 20) {
                            Picker("", selection: $segment) {
                                ForEach(BetSegment.allCases, id: \.self) { seg in
                                    Text(seg.rawValue).tag(seg)
                                }
                            }
                            .pickerStyle(.segmented)
                            .padding(.horizontal, 16)

                            if segment == .juice {
                                juiceContent
                            } else {
                                betContent
                            }
                        }
                        .padding(.top, 16)
                        .padding(.bottom, 32)
                    }
                    .refreshable {
                        if segment == .juice {
                            await fetchJuiceData()
                        } else {
                            await fetchData()
                        }
                    }
                }
            }
            .tabToolbar()
            .task { await fetchData() }
            .task { await loadJuiceSyndicates() }
            .sheet(isPresented: $showTeamPicker) {
                teamPickerSheet
            }
            .confirmationDialog(
                confirmOption?.name ?? "",
                isPresented: Binding(get: { confirmOption != nil }, set: { if !$0 { confirmOption = nil } }),
                titleVisibility: .visible
            ) {
                Button("Choose Enhancement") {
                    guard let opt = confirmOption else { return }
                    Task { await submitEnhancement(opt, teamId: 0) }
                    confirmOption = nil
                }
                Button("Cancel", role: .cancel) { confirmOption = nil }
            } message: {
                if let opt = confirmOption {
                    Text(opt.description)
                }
            }
        }
    }

    // MARK: - Bet content (Active / History)

    private var betContent: some View {
        Group {
            if displayBets.isEmpty {
                Text(segment == .active ? "No active bets" : "No bet history")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .padding(.top, 40)
            } else {
                VStack(alignment: .leading, spacing: 20) {
                    HStack {
                        Text(segment == .active ? "Active Bets" : "Bet History")
                            .font(.title3.weight(.bold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                        Spacer()
                        Text("\(displayBets.count)")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.secondary)
                    }
                    .padding(.horizontal, 16)

                    ForEach(syndicateGroups, id: \.syndicateId) { group in
                        VStack(alignment: .leading, spacing: 10) {
                            let syndicate = syndicates[group.syndicateId]
                            HStack(spacing: 6) {
                                Image(systemName: syndicate?.symbol ?? "house.fill")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(ProfileOption.color(for: syndicate?.color ?? ""))
                                Text(syndicate?.name ?? "Syndicate \(group.syndicateId)")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(.secondary)
                            }
                            .padding(.horizontal, 4)

                            ForEach(group.singles) { txn in
                                CardPlacedBet(
                                    txn: txn,
                                    onCancel: segment == .active ? { cancelBet(txn) } : nil
                                )
                            }

                            ForEach(group.parlays, id: \.first?.parlayId) { legs in
                                CardPlacedParlay(
                                    legs: legs,
                                    onCancel: segment == .active ? { cancelParlay(legs) } : nil
                                )
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                }
            }
        }
    }

    // MARK: - Juice content

    private var juiceContent: some View {
        VStack(alignment: .leading, spacing: 20) {
            // Syndicate picker
            if juiceSyndicates.count > 1 {
                Picker("Syndicate", selection: $juiceSyndicateId) {
                    ForEach(juiceSyndicates) { syn in
                        Text(syn.name).tag(syn.syndicateId)
                    }
                }
                .pickerStyle(.menu)
                .padding(.horizontal, 16)
                .onChange(of: juiceSyndicateId) { _, _ in Task { await fetchJuiceData() } }
            }

            if isLoadingJuice {
                ProgressView().frame(maxWidth: .infinity).padding(.top, 20)
            } else if juiceSyndicateId == 0 {
                Text("No syndicates found.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity)
                    .padding(.top, 40)
            } else {
                // Available enhancements
                if !uniqueOptions.isEmpty {
                    VStack(alignment: .leading, spacing: 12) {
                        SectionHeader("Enhancements")

                        if !clvOptions.isEmpty {
                            EnhancementGroupHeader("CLV", color: .blue)
                            ForEach(clvOptions) { opt in
                                EnhancementCard(option: opt, isChosen: chosenOptionHashes.contains(opt.optionHash)) {
                                    confirmOption = opt
                                }
                            }
                        }

                        if !teamOptions.isEmpty {
                            EnhancementGroupHeader("Team", color: .green)
                            ForEach(teamOptions) { opt in
                                EnhancementCard(option: opt, isChosen: chosenOptionHashes.contains(opt.optionHash)) {
                                    Task { await openTeamPicker(for: opt) }
                                }
                            }
                        }

                        if !edgeOptions.isEmpty {
                            EnhancementGroupHeader("Edge", color: .orange)
                            ForEach(edgeOptions) { opt in
                                EnhancementCard(option: opt, isChosen: chosenOptionHashes.contains(opt.optionHash)) {
                                    confirmOption = opt
                                }
                            }
                        }
                    }
                    .padding(.horizontal, 16)
                }

                // My enhancements
                if !myEnhanced.isEmpty {
                    VStack(alignment: .leading, spacing: 10) {
                        SectionHeader("My Enhancements")
                        ForEach(myEnhanced) { item in
                            ActiveEnhancementRow(
                                name: resolvedName(for: item),
                                type: resolvedType(for: item),
                                teamId: item.teamId,
                                teams: teamPickerTeams
                            )
                        }
                    }
                    .padding(.horizontal, 16)
                }

                // Syndicate enhancements (others)
                let othersEnhanced = syndicateEnhanced.filter { $0.bettorId != bettorId }
                if !othersEnhanced.isEmpty {
                    VStack(alignment: .leading, spacing: 10) {
                        SectionHeader("Syndicate")
                        ForEach(othersEnhanced) { item in
                            HStack(spacing: 8) {
                                EnhancementTypeBadge(type: resolvedType(for: item))
                                VStack(alignment: .leading, spacing: 2) {
                                    Text(resolvedName(for: item))
                                        .font(.subheadline.weight(.semibold))
                                        .foregroundStyle(theme.primaryText(colorScheme))
                                    Text("Runner \(item.bettorId)")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                            }
                            .padding(12)
                            .background(theme.cardBackground(colorScheme))
                            .clipShape(RoundedRectangle(cornerRadius: 12))
                        }
                    }
                    .padding(.horizontal, 16)
                }

                if uniqueOptions.isEmpty && myEnhanced.isEmpty && othersEnhanced.isEmpty {
                    Text("No enhancements available.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 20)
                }
            }
        }
    }

    // MARK: - Team picker sheet

    private var teamPickerSheet: some View {
        NavigationStack {
            Group {
                if isLoadingTeams {
                    ProgressView()
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    List(teamPickerTeams) { team in
                        Button {
                            guard let opt = pendingOption else { return }
                            showTeamPicker = false
                            Task { await submitEnhancement(opt, teamId: team.id) }
                            pendingOption = nil
                        } label: {
                            VStack(alignment: .leading, spacing: 4) {
                                Text(team.abbr)
                                    .font(.headline)
                                    .foregroundStyle(team.teamColor)
                                Text(team.name)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                TeamMetaRow(team: team)
                            }
                            .padding(.vertical, 4)
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
            .navigationTitle(pendingOption?.name ?? "Choose Team")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") {
                        showTeamPicker = false
                        pendingOption = nil
                    }
                }
            }
        }
    }

    // MARK: - Helpers

    @ViewBuilder
    private func SectionHeader(_ title: String) -> some View {
        Text(title)
            .font(.title3.weight(.bold))
            .foregroundStyle(theme.primaryText(colorScheme))
    }

    // MARK: - Actions

    private func openTeamPicker(for option: EnhanceOption) async {
        pendingOption = option
        isLoadingTeams = true
        showTeamPicker = true
        teamPickerTeams = (try? await teamService.fetchTeams(leagueId: option.leagueId)) ?? []
        isLoadingTeams = false
    }

    private func submitEnhancement(_ option: EnhanceOption, teamId: Int) async {
        guard let result = try? await enhancementService.chooseEnhancement(
            bettorId: bettorId,
            syndicateId: juiceSyndicateId,
            enhancementId: option.enhancementId,
            teamId: teamId,
            level: 1,
            optionHash: option.optionHash
        ) else { return }
        myEnhanced.append(result)
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

    private func fetchData() async {
        guard bettorId != 0 else { return }
        isLoading = txnRecords.isEmpty && completedRecords.isEmpty
        defer { isLoading = false }
        async let activeFetch = txnService.fetchActiveBets(bettorId: bettorId)
        async let completedFetch = txnService.fetchCompletedBets(bettorId: bettorId)
        txnRecords = (try? await activeFetch) ?? []
        completedRecords = (try? await completedFetch) ?? []
        let ids = Set((txnRecords + completedRecords).map(\.syndicateId))
        for sid in ids where syndicates[sid] == nil {
            if let result = try? await syndicateService.fetchSyndicate(syndicateId: sid, bettorId: nil) {
                syndicates[sid] = result.first
            }
        }
    }

    private func loadJuiceSyndicates() async {
        guard bettorId != 0 else { return }
        juiceSyndicates = (try? await syndicateService.fetchSyndicate(bettorId: bettorId)) ?? []
        if juiceSyndicateId == 0, let first = juiceSyndicates.first {
            juiceSyndicateId = first.syndicateId
            await fetchJuiceData()
        }
    }

    private func fetchJuiceData() async {
        guard juiceSyndicateId != 0, bettorId != 0 else { return }
        isLoadingJuice = true
        defer { isLoadingJuice = false }
        async let optsFetch = enhancementService.fetchOptions(bettorId: bettorId, syndicateId: juiceSyndicateId)
        async let myFetch   = enhancementService.fetchEnhanced(bettorId: bettorId, syndicateId: juiceSyndicateId)
        async let syndFetch = enhancementService.fetchEnhanced(syndicateId: juiceSyndicateId)
        availableOptions    = (try? await optsFetch) ?? []
        myEnhanced          = (try? await myFetch) ?? []
        syndicateEnhanced   = (try? await syndFetch) ?? []
    }
}

// MARK: - Sub-views

private struct EnhancementGroupHeader: View {
    let title: String
    let color: Color
    init(_ title: String, color: Color) { self.title = title; self.color = color }

    var body: some View {
        HStack(spacing: 6) {
            Capsule()
                .fill(color.opacity(0.2))
                .frame(width: 4, height: 14)
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(color)
        }
        .padding(.top, 4)
    }
}

private struct EnhancementTypeBadge: View {
    let type: String

    var badgeColor: Color {
        switch type {
        case "clv":  return .blue
        case "team": return .green
        case "edge": return .orange
        default:     return .secondary
        }
    }

    var body: some View {
        Text(type.uppercased())
            .font(.system(size: 9, weight: .bold))
            .foregroundStyle(badgeColor)
            .padding(.horizontal, 6)
            .padding(.vertical, 3)
            .background(badgeColor.opacity(0.15))
            .clipShape(Capsule())
    }
}

private struct EnhancementCard: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let option: EnhanceOption
    let isChosen: Bool
    let onTap: () -> Void

    var body: some View {
        Button(action: { if !isChosen { onTap() } }) {
            HStack(spacing: 12) {
                EnhancementTypeBadge(type: option.enhancementType)

                VStack(alignment: .leading, spacing: 3) {
                    HStack(spacing: 6) {
                        Text(option.name)
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                        if let betType = option.betType {
                            Text(betType.uppercased())
                                .font(.system(size: 9, weight: .medium))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 5)
                                .padding(.vertical, 2)
                                .background(Color.secondary.opacity(0.12))
                                .clipShape(Capsule())
                        }
                    }
                    Text(option.description)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(2)
                }

                Spacer()

                if isChosen {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundStyle(.green)
                } else {
                    Image(systemName: "plus.circle")
                        .foregroundStyle(.secondary)
                }
            }
            .padding(12)
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 12))
            .opacity(isChosen ? 0.6 : 1.0)
        }
        .buttonStyle(.plain)
        .disabled(isChosen)
    }
}

private struct ActiveEnhancementRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let name: String
    let type: String
    let teamId: Int?
    let teams: [Team]

    private var teamAbbr: String? {
        guard let tid = teamId, tid != 0 else { return nil }
        return teams.first { $0.id == tid }?.abbr
    }

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
            EnhancementTypeBadge(type: type)
            VStack(alignment: .leading, spacing: 2) {
                Text(name)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let abbr = teamAbbr {
                    Text(abbr)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            Spacer()
        }
        .padding(12)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }
}

#Preview {
    TabJuiceView()
        .environmentObject(AppTheme())
}
