import SwiftUI

struct TabJuiceView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId") private var bettorId: Int = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int = 0

    @State private var txnRecords: [Txn] = []
    @State private var syndicates: [Int: Syndicate] = [:]
    @State private var isLoading = false
    @State private var segment: BetSegment = .slips

    // Juice state
    @State private var juiceSyndicates: [Syndicate] = []
    @State private var juiceSyndicateId: Int = 0
    @State private var syndicateEnhanced: [Enhanced] = []
    @State private var isLoadingJuice = false
    @State private var showAddJuice = false

    private let txnService = TxnService()
    private let syndicateService = SyndicateService()
    private let enhancementService = EnhancementService()

    private enum BetSegment: String, CaseIterable {
        case slips = "Slips"
        case juice = "Juice"
    }

    private var activeBets: [Txn] {
        txnRecords.filter { $0.canceled != true }
    }

    private var syndicateGroups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]])] {
        let bySyndicate = Dictionary(grouping: activeBets, by: \.syndicateId)
        return bySyndicate.keys.sorted().map { sid in
            let group = bySyndicate[sid] ?? []
            let singles = group.filter { $0.parlayId == nil }
            let parlayMap = Dictionary(grouping: group.filter { $0.parlayId != nil }, by: { $0.parlayId! })
            let parlays = parlayMap.values.map { $0 }
            return (syndicateId: sid, singles: singles, parlays: parlays)
        }
    }

    private var juiceGroups: [(syndicateId: Int, team: [Enhanced], clv: [Enhanced], others: [Enhanced])] {
        let mine = syndicateEnhanced.filter { $0.bettorId == bettorId }
        let others = syndicateEnhanced.filter { $0.bettorId != bettorId }
        let sids = Set(mine.map(\.syndicateId)).union(others.map(\.syndicateId))
        return sids.sorted().map { sid in
            let mineIn = mine.filter { $0.syndicateId == sid }
            return (
                syndicateId: sid,
                team: mineIn.filter { $0.enhancementType != "clv" }.sorted { $0.name < $1.name },
                clv: mineIn.filter { $0.enhancementType == "clv" },
                others: others.filter { $0.syndicateId == sid }
            )
        }
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
            .sheet(isPresented: $showAddJuice, onDismiss: { Task { await fetchJuiceData() } }) {
                SheetAddJuice(bettorId: bettorId, syndicates: juiceSyndicates, syndicateId: juiceSyndicateId)
            }
        }
    }

    // MARK: - Bet content (Slips)

    private var betContent: some View {
        Group {
            if activeBets.isEmpty {
                Text("No active bets")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .padding(.top, 40)
            } else {
                VStack(alignment: .leading, spacing: 20) {
                    HStack {
                        Text("Active Bets")
                            .font(.title3.weight(.bold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                        Spacer()
                        Text("\(activeBets.count)")
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
                                    onCancel: { cancelBet(txn) }
                                )
                            }

                            ForEach(group.parlays, id: \.first?.parlayId) { legs in
                                CardPlacedParlay(
                                    legs: legs,
                                    onCancel: { cancelParlay(legs) }
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
            if isLoadingJuice {
                ProgressView().frame(maxWidth: .infinity).padding(.top, 20)
            } else if juiceSyndicates.isEmpty {
                Text("No syndicates found.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity)
                    .padding(.top, 40)
            } else {
                Button {
                    showAddJuice = true
                } label: {
                    Label("Add Juice", systemImage: "syringe.fill")
                        .font(.subheadline.weight(.semibold))
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .background(theme.accent.opacity(0.15))
                        .foregroundStyle(theme.accent)
                        .clipShape(RoundedRectangle(cornerRadius: 12))
                        .overlay(RoundedRectangle(cornerRadius: 12).stroke(theme.accent.opacity(0.35), lineWidth: 1))
                }
                .padding(.horizontal, 16)

                if juiceGroups.isEmpty {
                    Text("No enhancements yet. Tap Add Juice to choose one.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 20)
                } else {
                    ForEach(juiceGroups, id: \.syndicateId) { group in
                        VStack(alignment: .leading, spacing: 10) {
                            let syndicate = juiceSyndicates.first { $0.syndicateId == group.syndicateId }
                            HStack(spacing: 6) {
                                Image(systemName: syndicate?.symbol ?? "house.fill")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(ProfileOption.color(for: syndicate?.color ?? ""))
                                Text(syndicate?.name ?? "Syndicate \(group.syndicateId)")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(.secondary)
                            }
                            .padding(.horizontal, 4)

                            if !group.team.isEmpty {
                                VStack(alignment: .leading, spacing: 8) {
                                    ForEach(group.team) { item in
                                        EnhancedLevelCapsule(item: item)
                                    }
                                }
                            }

                            if !group.clv.isEmpty {
                                CLVLevelLine(items: group.clv)
                            }

                            if !group.others.isEmpty {
                                VStack(alignment: .leading, spacing: 8) {
                                    ForEach(group.others) { item in
                                        HStack(spacing: 8) {
                                            EnhancementTypeBadge(type: item.enhancementType)
                                            Text(item.name)
                                                .font(.subheadline.weight(.semibold))
                                                .foregroundStyle(theme.primaryText(colorScheme))
                                            Spacer()
                                            Text("Runner \(item.bettorId)")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        }
                                        .padding(12)
                                        .background(theme.cardBackground(colorScheme))
                                        .clipShape(RoundedRectangle(cornerRadius: 12))
                                    }
                                }
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                }
            }
        }
    }

    // MARK: - Actions

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
        isLoading = txnRecords.isEmpty
        defer { isLoading = false }
        txnRecords = (try? await txnService.fetchActiveBets(bettorId: bettorId)) ?? []
        let ids = Set(txnRecords.map(\.syndicateId))
        for sid in ids where syndicates[sid] == nil {
            if let result = try? await syndicateService.fetchSyndicate(syndicateId: sid, bettorId: nil) {
                syndicates[sid] = result.first
            }
        }
    }

    private func loadJuiceSyndicates() async {
        guard bettorId != 0 else { return }
        juiceSyndicates = (try? await syndicateService.fetchSyndicate(bettorId: bettorId)) ?? []
        if juiceSyndicateId == 0 {
            if juiceSyndicates.contains(where: { $0.syndicateId == selectedSyndicateId }) {
                juiceSyndicateId = selectedSyndicateId
            } else if let first = juiceSyndicates.first {
                juiceSyndicateId = first.syndicateId
            }
        }
        await fetchJuiceData()
    }

    private func fetchJuiceData() async {
        guard bettorId != 0, !juiceSyndicates.isEmpty else { return }
        isLoadingJuice = true
        defer { isLoadingJuice = false }
        var combined: [Enhanced] = []
        for syn in juiceSyndicates {
            if let items = try? await enhancementService.fetchEnhanced(syndicateId: syn.syndicateId) {
                combined.append(contentsOf: items)
            }
        }
        syndicateEnhanced = combined
    }
}

// MARK: - Sub-views

private struct EnhancedLevelCapsule: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let item: Enhanced

    var body: some View {
        HStack(spacing: 8) {
            if item.enhancementType == "team" {
                Image(systemName: League.sportIcon(for: item.leagueId ?? 0))
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            Text(item.name)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            Spacer()
            Text("Level \(item.level)")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            Image(systemName: "nairasign.circle.fill")
                .font(.caption)
                .foregroundStyle(theme.accent)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(theme.cardBackground(colorScheme))
        .clipShape(Capsule())
    }
}

private struct CLVLevelLine: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let items: [Enhanced]

    private static let order = ["ML", "SPR", "O/U"]

    private func level(for name: String) -> Int? {
        items.first { $0.name == name }?.level
    }

    private func multiplierLabel(_ level: Int?) -> String {
        guard let level, level > 0 else { return "1x" }
        return String(format: "%.1fx", Double(level) * 0.1 + 1.0)
    }

    var body: some View {
        HStack(spacing: 10) {
            ForEach(Self.order, id: \.self) { name in
                VStack(spacing: 2) {
                    Text(name)
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text(multiplierLabel(level(for: name)))
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(theme.accent)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
                .background(theme.cardBackground(colorScheme))
                .clipShape(Capsule())
            }
        }
    }
}

#Preview {
    TabJuiceView()
        .environmentObject(AppTheme())
}
