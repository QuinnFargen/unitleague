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
    @State private var availableOptions: [EnhanceOption] = []
    @State private var myEnhanced: [Enhanced] = []
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
            .sheet(isPresented: $showAddJuice, onDismiss: { Task { await fetchJuiceData() } }) {
                SheetAddJuice(bettorId: bettorId, syndicateId: juiceSyndicateId)
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

                // My enhancements
                if !myEnhanced.isEmpty {
                    VStack(alignment: .leading, spacing: 10) {
                        SectionHeader("My Enhancements")
                        ForEach(myEnhanced) { item in
                            ActiveEnhancementRow(
                                name: resolvedName(for: item),
                                type: resolvedType(for: item),
                                teamAbbr: item.teamAbbr
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

                if myEnhanced.isEmpty && othersEnhanced.isEmpty {
                    Text("No enhancements yet. Tap Add Juice to choose one.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 20)
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
            if juiceSyndicateId != 0 {
                await fetchJuiceData()
            }
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

private struct ActiveEnhancementRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let name: String
    let type: String
    let teamAbbr: String?

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
            EnhancementTypeBadge(type: type)
            VStack(alignment: .leading, spacing: 2) {
                Text(name)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let teamAbbr {
                    Text(teamAbbr)
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
