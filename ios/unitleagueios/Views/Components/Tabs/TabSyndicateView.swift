import SwiftUI

struct TabSyndicateView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId")            private var bettorId: Int = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int = 0
    @AppStorage("profileSymbol")       private var profileSymbol: String = ProfileOption.symbols[0]

    @State private var syndicates: [Syndicate] = SyndicateCache.load()
    @State private var isLoading = false
    @State private var fetchError: String?
    @State private var showingJoin = false
    @State private var showingCreate = false
    @State private var isEditing = false

    private let service = SyndicateService()

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

    private var visibleSyndicates: [Syndicate] {
        syndicates
            .filter { !$0.isHidden }
            .sorted { ($0.sortOrder ?? .max, $0.syndicateId) < ($1.sortOrder ?? .max, $1.syndicateId) }
    }

    private var hiddenSyndicates: [Syndicate] {
        syndicates.filter(\.isHidden)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                VStack(spacing: 16) {
                    HStack(spacing: 12) {
                        LeagueActionButton(title: "Join", icon: "person.badge.plus", tint: theme.accent) {
                            showingJoin = true
                        }
                        LeagueActionButton(title: "Create", icon: "plus.circle", tint: theme.accent) {
                            showingCreate = true
                        }
                        EditOrderButton(isEditing: isEditing, tint: theme.accent) {
                            withAnimation { isEditing.toggle() }
                        }
                    }
                    .fixedSize(horizontal: false, vertical: true)
                    .padding(.horizontal, 32)
                    .padding(.top, 16)

                    if isEditing {
                        editList
                    } else {
                        syndicateList
                    }
                }
            }
            .tabToolbar()
            .task { await load() }
            .sheet(isPresented: $showingJoin, onDismiss: { Task { await load() } }) {
                SheetSyndicateJoin(bettorId: bettorId)
            }
            .sheet(isPresented: $showingCreate, onDismiss: { Task { await load() } }) {
                SheetSyndicateCreate(bettorId: bettorId)
            }
        }
    }

    private var syndicateList: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 10) {
                Text("Syndicates")
                    .font(.headline)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)

                NavigationLink(destination: ViewSyndicate(syndicate: soloSyndicate)) {
                    CardSyndicate(
                        syndicate: soloSyndicate,
                        style: .compact,
                        isSelected: selectedSyndicateId == 0
                    )
                }
                .buttonStyle(.plain)

                if isLoading {
                    ProgressView().frame(maxWidth: .infinity).padding(.top, 20)
                } else if let error = fetchError {
                    Text(error)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 20)
                } else if !visibleSyndicates.isEmpty {
                    ForEach(visibleSyndicates) { syndicate in
                        NavigationLink(destination: ViewSyndicate(syndicate: syndicate)) {
                            CardSyndicate(
                                syndicate: syndicate,
                                style: .compact,
                                isSelected: syndicate.syndicateId == selectedSyndicateId
                            )
                        }
                        .buttonStyle(.plain)
                    }
                } else {
                    Text(syndicates.isEmpty ? "You're not in any syndicates yet." : "All your syndicates are hidden.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 20)
                }
            }
            .padding(.horizontal, 32)
            .padding(.bottom, 32)
        }
    }

    /// Drag to reorder; eye buttons hide/unhide. Hidden syndicates stay listed here so they can be added back.
    private var editList: some View {
        List {
            Section {
                ForEach(visibleSyndicates) { syndicate in
                    editRow(syndicate, hidden: false)
                }
                .onMove(perform: move)
            } header: {
                sectionHeader("Syndicates")
            }

            if !hiddenSyndicates.isEmpty {
                Section {
                    ForEach(hiddenSyndicates) { syndicate in
                        editRow(syndicate, hidden: true)
                            .moveDisabled(true)
                    }
                } header: {
                    sectionHeader("Hidden")
                }
            }

            if let error = fetchError {
                Text(error)
                    .font(.caption)
                    .foregroundStyle(theme.error)
                    .listRowBackground(Color.clear)
                    .listRowSeparator(.hidden)
            }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
        .environment(\.editMode, .constant(.active))
    }

    private func sectionHeader(_ title: String) -> some View {
        Text(title)
            .font(.headline)
            .foregroundStyle(.secondary)
            .textCase(nil)
    }

    private func editRow(_ syndicate: Syndicate, hidden: Bool) -> some View {
        HStack(spacing: 12) {
            CardSyndicate(syndicate: syndicate, style: .compact)
                .opacity(hidden ? 0.5 : 1)

            Button {
                withAnimation { hidden ? unhide(syndicate) : hide(syndicate) }
            } label: {
                Image(systemName: hidden ? "eye" : "eye.slash")
                    .font(.body)
                    .foregroundStyle(hidden ? theme.accent : .secondary)
                    .frame(width: 32, height: 32)
            }
            .buttonStyle(.borderless)
        }
        .listRowBackground(Color.clear)
        .listRowSeparator(.hidden)
        .listRowInsets(EdgeInsets(top: 5, leading: 32, bottom: 5, trailing: 16))
    }

    // MARK: - Ordering

    private func move(from source: IndexSet, to destination: Int) {
        var ids = visibleSyndicates.map(\.syndicateId)
        ids.move(fromOffsets: source, toOffset: destination)
        applyOrder(ids)
    }

    private func hide(_ syndicate: Syndicate) {
        applyOrder(visibleSyndicates.map(\.syndicateId).filter { $0 != syndicate.syndicateId })
    }

    private func unhide(_ syndicate: Syndicate) {
        applyOrder(visibleSyndicates.map(\.syndicateId) + [syndicate.syndicateId])
    }

    /// `visibleIds` in display order; every other syndicate becomes hidden. Updates locally, then saves.
    private func applyOrder(_ visibleIds: [Int]) {
        syndicates = syndicates.map { syndicate in
            var s = syndicate
            s.sortOrder = visibleIds.firstIndex(of: s.syndicateId).map { $0 + 1 }
            return s
        }
        SyndicateCache.save(syndicates)
        fetchError = nil
        Task {
            do {
                try await service.updateRunnerOrder(bettorId: bettorId, syndicateIds: visibleIds)
            } catch {
                fetchError = error.localizedDescription
                await load()
            }
        }
    }

    private func load() async {
        guard bettorId > 0 else { return }
        isLoading = syndicates.isEmpty
        fetchError = nil
        do {
            let raw = try await service.fetchSyndicate(bettorId: bettorId)
            var seen = Set<Int>()
            syndicates = raw.filter { seen.insert($0.syndicateId).inserted }
            SyndicateCache.save(syndicates)
        } catch {
            fetchError = error.localizedDescription
        }
        isLoading = false
    }
}

// MARK: - Sub-views

private struct LeagueActionButton: View {
    let title: String
    let icon: String
    let tint: Color
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .font(.subheadline)
                .fontWeight(.semibold)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .padding(.vertical, 14)
                .background(tint.opacity(0.15))
                .foregroundStyle(tint)
                .clipShape(RoundedRectangle(cornerRadius: 12))
                .overlay(RoundedRectangle(cornerRadius: 12).stroke(tint.opacity(0.35), lineWidth: 1))
        }
    }
}

/// Symbol-only button beside Join/Create that toggles reorder/hide mode.
private struct EditOrderButton: View {
    let isEditing: Bool
    let tint: Color
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Image(systemName: "slider.vertical.3")
                .font(.subheadline)
                .fontWeight(.semibold)
                .frame(width: 48)
                .frame(maxHeight: .infinity)
                .padding(.vertical, 14)
                .background(tint.opacity(isEditing ? 0.35 : 0.15))
                .foregroundStyle(tint)
                .clipShape(RoundedRectangle(cornerRadius: 12))
                .overlay(RoundedRectangle(cornerRadius: 12).stroke(tint.opacity(isEditing ? 0.8 : 0.35), lineWidth: 1))
        }
        .accessibilityLabel(isEditing ? "Done editing syndicates" : "Edit syndicates")
    }
}

enum SyndicateOption {
    static let symbols = [
        "person.3.fill",
        "house.fill",
        "bitcoinsign.bank.building.fill",
        "brain.filled.head.profile",
        "puzzlepiece.fill",
        "lock.fill",
        "fuelpump.fill",
        "rainbow",
        "carbon.dioxide.cloud.fill",
        "trash.fill",
        "receipt.fill",
        "graduationcap.fill",
        "dumbbell.fill",
        "fire.extinguisher.fill",
        "trophy.fill",
        "wallet.bifold.fill",
        "hammer.fill",
        "lightbulb.fill",
        "toilet.fill",
        "crown.fill",
        "ivfluid.bag",
        "shippingbox.fill",
        "eurozonesign.bank.building.fill",
        "bolt.fill",
        "arrowkeys.fill",
        "airplane.up.forward",
        "ev.charger.fill",
        "engine.combustion.fill",
        "exclamationmark.tirepressure",
        "figure.seated.seatbelt",
        "minus.plus.batteryblock.fill",
        "hare.fill",
        "tortoise.fill",
        "lungs.fill",
        "mustache.fill",
        "brain.fill",
        "poweroutlet.type.b.fill",
        "popcorn.fill",
        "gamecontroller.fill",
        "creditcard.fill",
        "dollarsign",
        "alarm.fill",
        "cross.case.fill",
        "pills.fill",
        "pi.square.fill"
    ]
}

private enum SyndicateCache {
    private static let key = "cachedSyndicates_v2" // v2: includes sort_order (nil = hidden)

    static func load() -> [Syndicate] {
        guard let data = UserDefaults.standard.data(forKey: key),
              let saved = try? JSONDecoder().decode([Syndicate].self, from: data)
        else { return [] }
        return saved
    }

    static func save(_ items: [Syndicate]) {
        guard let data = try? JSONEncoder().encode(items) else { return }
        UserDefaults.standard.set(data, forKey: key)
    }
}

#Preview {
    TabSyndicateView()
        .environmentObject(AppTheme())
}
