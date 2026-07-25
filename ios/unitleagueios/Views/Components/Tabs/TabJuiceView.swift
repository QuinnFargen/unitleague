import SwiftUI

struct TabJuiceView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId") private var bettorId: Int = 0

    @State private var myRunners: [Int: Runner] = [:]

    // Juice state
    @State private var juiceSyndicates: [Syndicate] = []
    @State private var syndicateEnhanced: [Enhanced] = JuiceCache.load()
    @State private var isLoadingJuice = false
    @State private var showAddJuice = false

    private let syndicateService = SyndicateService()
    private let enhancementService = EnhancementService()
    private let runnerService = RunnerService()

    private var juiceGroups: [(syndicateId: Int, team: [Enhanced], edge: [Enhanced], clv: [Enhanced], others: [Enhanced])] {
        let mine = syndicateEnhanced.filter { $0.bettorId == bettorId }
        let others = syndicateEnhanced.filter { $0.bettorId != bettorId }
        let sids = Set(mine.map(\.syndicateId)).union(others.map(\.syndicateId))
        return sids.sorted().map { sid in
            let mineIn = mine.filter { $0.syndicateId == sid }
            return (
                syndicateId: sid,
                team: mineIn.filter { $0.enhancementType == "team" }.sorted { $0.name < $1.name },
                edge: mineIn.filter { $0.enhancementType == "edge" }.sorted { $0.name < $1.name },
                clv: mineIn.filter { $0.enhancementType == "clv" },
                others: others.filter { $0.syndicateId == sid }
            )
        }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    juiceContent
                        .padding(.top, 16)
                        .padding(.bottom, 32)
                }
                .refreshable {
                    await fetchJuiceData()
                }
            }
            .tabToolbar()
            .task { await loadJuiceSyndicates() }
            .task { await loadRunners() }
            .sheet(isPresented: $showAddJuice, onDismiss: { Task { await fetchJuiceData() } }) {
                SheetAddJuice(bettorId: bettorId, syndicates: juiceSyndicates)
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
                            SyndicateHeaderRow(syndicate: syndicate, syndicateId: group.syndicateId, runner: myRunners[group.syndicateId], bettorId: bettorId)

                            if !group.team.isEmpty {
                                let teamsByLeague = Dictionary(grouping: group.team, by: { $0.leagueId ?? 0 })
                                VStack(alignment: .leading, spacing: 10) {
                                    ForEach(teamsByLeague.keys.sorted(), id: \.self) { lid in
                                        HStack(alignment: .center, spacing: 12) {
                                            Image(systemName: League.sportIcon(for: lid))
                                                .font(.title2)
                                                .foregroundStyle(theme.primaryText(colorScheme))
                                                .frame(width: 44, height: 44)
                                                .background(theme.cardBackground(colorScheme))
                                                .clipShape(RoundedRectangle(cornerRadius: 10))

                                            ScrollView(.horizontal, showsIndicators: false) {
                                                HStack(spacing: 8) {
                                                    ForEach(teamsByLeague[lid] ?? []) { item in
                                                        TeamLevelCapsule(item: item)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if !group.clv.isEmpty {
                                CLVLevelLine(items: group.clv)
                            }

                            if !group.edge.isEmpty {
                                VStack(alignment: .leading, spacing: 8) {
                                    ForEach(group.edge) { item in
                                        HStack(spacing: 8) {
                                            Image(systemName: item.symbol ?? "bolt.fill")
                                                .font(.subheadline)
                                                .foregroundStyle(theme.accent)
                                            Text(item.name)
                                                .font(.subheadline.weight(.semibold))
                                                .foregroundStyle(theme.primaryText(colorScheme))
                                            Spacer()
                                        }
                                        .padding(12)
                                        .background(theme.cardBackground(colorScheme))
                                        .clipShape(RoundedRectangle(cornerRadius: 12))
                                    }
                                }
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

    private func loadRunners() async {
        guard bettorId != 0 else { return }
        let fetched = (try? await runnerService.fetchRunner(bettorId: bettorId)) ?? []
        var map: [Int: Runner] = [:]
        for runner in fetched where runner.bettorId == bettorId {
            map[runner.syndicateId] = runner
        }
        myRunners = map
    }

    private func loadJuiceSyndicates() async {
        guard bettorId != 0 else { return }
        juiceSyndicates = (try? await syndicateService.fetchSyndicate(bettorId: bettorId)) ?? []
        await fetchJuiceData()
    }

    private func fetchJuiceData() async {
        guard bettorId != 0, !juiceSyndicates.isEmpty else { return }
        isLoadingJuice = syndicateEnhanced.isEmpty
        defer { isLoadingJuice = false }
        var combined: [Enhanced] = []
        for syn in juiceSyndicates {
            if let items = try? await enhancementService.fetchEnhanced(syndicateId: syn.syndicateId) {
                combined.append(contentsOf: items)
            }
        }
        syndicateEnhanced = combined
        JuiceCache.save(combined)
    }
}

// MARK: - Sub-views

private struct TeamLevelCapsule: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let item: Enhanced

    var body: some View {
        HStack(spacing: 6) {
            Text(item.name)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            Text("\(item.level)")
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

    private func multiplier(_ level: Int?) -> Double {
        guard let level, level > 0 else { return 1.0 }
        return Double(level) * 0.1 + 1.0
    }

    private func multiplierLabel(_ multiplier: Double) -> String {
        String(format: "%.1fx", multiplier)
    }

    var body: some View {
        HStack(spacing: 10) {
            ForEach(Self.order, id: \.self) { name in
                let mult = multiplier(level(for: name))
                HStack(spacing: 4) {
                    Text(name)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text(multiplierLabel(mult))
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
                .background(theme.cardBackground(colorScheme))
                .clipShape(Capsule())
            }
        }
    }
}

private enum JuiceCache {
    private static let key = "cachedSyndicateEnhanced"

    static func load() -> [Enhanced] {
        guard let data = UserDefaults.standard.data(forKey: key),
              let saved = try? JSONDecoder().decode([Enhanced].self, from: data)
        else { return [] }
        return saved
    }

    static func save(_ items: [Enhanced]) {
        guard let data = try? JSONEncoder().encode(items) else { return }
        UserDefaults.standard.set(data, forKey: key)
    }
}

#Preview {
    TabJuiceView()
        .environmentObject(AppTheme())
}
