import SwiftUI

struct SheetSyndicateRules: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @Binding var syndicate: Syndicate
    let isAdmin: Bool

    @State private var selectedLeagueIds: Set<Int>
    @State private var blockedTypes: Set<String>
    @State private var blockedEdgeIds: Set<Int>
    @State private var maxClvLevel: Int
    @State private var maxTeamLevel: Int
    @State private var minUnitsWagered: Int
    @State private var minWagers: Int

    @State private var leagues: [League] = []
    @State private var edges: [EnhancementDef] = []
    @State private var isLoading = false
    @State private var isSaving = false
    @State private var errorMessage: String?

    private let enhancementTypes = ["clv", "edge", "team"]

    init(syndicate: Binding<Syndicate>, isAdmin: Bool) {
        _syndicate = syndicate
        self.isAdmin = isAdmin
        let config = syndicate.wrappedValue.config
        _selectedLeagueIds = State(initialValue: Set(config?.leagueIds ?? []))
        _blockedTypes = State(initialValue: Set(config?.blockedEnhancementTypes ?? []))
        _blockedEdgeIds = State(initialValue: Set(config?.blockedEnhancementIds ?? []))
        _maxClvLevel = State(initialValue: config?.maxClvLevel ?? 0)
        _maxTeamLevel = State(initialValue: config?.maxTeamLevel ?? 0)
        _minUnitsWagered = State(initialValue: config?.minUnitsWagered ?? 0)
        _minWagers = State(initialValue: config?.minWagers ?? 0)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(alignment: .leading, spacing: 28) {
                        section(title: "Leagues") {
                            if isLoading && leagues.isEmpty {
                                ProgressView().frame(maxWidth: .infinity)
                            } else {
                                LazyVGrid(columns: [GridItem(.adaptive(minimum: 80), spacing: 8)], spacing: 8) {
                                    ForEach(leagues) { league in
                                        toggleChip(title: league.abbr, isOn: selectedLeagueIds.contains(league.id)) {
                                            if selectedLeagueIds.contains(league.id) {
                                                selectedLeagueIds.remove(league.id)
                                            } else {
                                                selectedLeagueIds.insert(league.id)
                                            }
                                        }
                                    }
                                }
                                Text("No leagues selected allows every league.")
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                        }

                        section(title: "Wager Minimums") {
                            VStack(spacing: 10) {
                                numberRow(title: "Min Units Wagered", value: $minUnitsWagered, range: 0...50, zeroLabel: "No minimum")
                                numberRow(title: "Min # of Wagers", value: $minWagers, range: 0...50, zeroLabel: "No minimum")
                            }
                        }

                        section(title: "Block Enhancement Types") {
                            HStack(spacing: 8) {
                                ForEach(enhancementTypes, id: \.self) { type in
                                    toggleChip(title: type.uppercased(), isOn: blockedTypes.contains(type)) {
                                        if blockedTypes.contains(type) {
                                            blockedTypes.remove(type)
                                        } else {
                                            blockedTypes.insert(type)
                                        }
                                    }
                                }
                            }
                        }

                        section(title: "Max Levels") {
                            VStack(spacing: 10) {
                                numberRow(title: "CLV Max Level", value: $maxClvLevel, range: 0...20, zeroLabel: "No limit")
                                numberRow(title: "Team Max Level", value: $maxTeamLevel, range: 0...20, zeroLabel: "No limit")
                            }
                        }

                        section(title: "Block Specific Edges") {
                            if isLoading && edges.isEmpty {
                                ProgressView().frame(maxWidth: .infinity)
                            } else if edges.isEmpty {
                                Text("No edge enhancements available.")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            } else {
                                VStack(spacing: 8) {
                                    ForEach(edges) { edge in
                                        checklistRow(
                                            title: edge.name,
                                            subtitle: edge.description,
                                            isSelected: blockedEdgeIds.contains(edge.enhancementId)
                                        ) {
                                            if blockedEdgeIds.contains(edge.enhancementId) {
                                                blockedEdgeIds.remove(edge.enhancementId)
                                            } else {
                                                blockedEdgeIds.insert(edge.enhancementId)
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if let error = errorMessage {
                            Text(error)
                                .font(.caption)
                                .foregroundStyle(theme.error)
                                .multilineTextAlignment(.center)
                        }

                        if isAdmin {
                            Button {
                                save()
                            } label: {
                                HStack(spacing: 8) {
                                    if isSaving { ProgressView().scaleEffect(0.8) }
                                    Text("Save Rules")
                                        .font(.body).fontWeight(.medium)
                                }
                                .foregroundStyle(theme.accent)
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 14)
                                .background(theme.accent.opacity(0.12))
                                .clipShape(RoundedRectangle(cornerRadius: 12))
                            }
                            .disabled(isSaving)
                        }
                    }
                    .padding(16)
                    .padding(.bottom, 24)
                }
            }
            .navigationTitle("Syndicate Rules")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(isAdmin ? "Cancel" : "Done") { dismiss() }
                }
            }
            .task { await load() }
        }
    }

    @ViewBuilder
    private func section<Content: View>(title: String, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            content()
        }
    }

    @ViewBuilder
    private func checklistRow(title: String, subtitle: String?, isSelected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .foregroundStyle(theme.primaryText(colorScheme))
                    if let subtitle, !subtitle.isEmpty {
                        Text(subtitle)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                Spacer()
                Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                    .foregroundStyle(isSelected ? theme.accent : .secondary)
            }
            .padding()
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
        }
        .buttonStyle(.plain)
        .disabled(!isAdmin)
    }

    @ViewBuilder
    private func toggleChip(title: String, isOn: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Text(title)
                .font(.subheadline.weight(isOn ? .semibold : .regular))
                .foregroundStyle(isOn ? theme.chipSelectedFG(colorScheme) : theme.primaryText(colorScheme))
                .padding(.horizontal, 16)
                .padding(.vertical, 8)
                .background(isOn ? theme.chipSelected(colorScheme) : theme.chipUnselected(colorScheme))
                .clipShape(Capsule())
        }
        .buttonStyle(.plain)
        .disabled(!isAdmin)
    }

    @ViewBuilder
    private func numberRow(title: String, value: Binding<Int>, range: ClosedRange<Int>, zeroLabel: String) -> some View {
        HStack {
            Text(title)
                .foregroundStyle(theme.primaryText(colorScheme))
            Spacer()
            Stepper(value: value, in: range) {
                Text(value.wrappedValue == 0 ? zeroLabel : "\(value.wrappedValue)")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .frame(minWidth: 80, alignment: .trailing)
            }
            .disabled(!isAdmin)
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    private func load() async {
        isLoading = true
        async let leaguesTask = LeagueService().fetchLeagues()
        async let edgesTask = EnhancementService().fetchEnhancements(type: "edge")
        leagues = (try? await leaguesTask) ?? []
        edges = (try? await edgesTask) ?? []
        isLoading = false
    }

    private func save() {
        isSaving = true
        errorMessage = nil
        let config = SyndicateConfig(
            leagueIds: selectedLeagueIds.isEmpty ? nil : Array(selectedLeagueIds).sorted(),
            blockedEnhancementTypes: blockedTypes.isEmpty ? nil : Array(blockedTypes).sorted(),
            blockedEnhancementIds: blockedEdgeIds.isEmpty ? nil : Array(blockedEdgeIds).sorted(),
            maxClvLevel: maxClvLevel == 0 ? nil : maxClvLevel,
            maxTeamLevel: maxTeamLevel == 0 ? nil : maxTeamLevel,
            minUnitsWagered: minUnitsWagered == 0 ? nil : minUnitsWagered,
            minWagers: minWagers == 0 ? nil : minWagers
        )
        let id = syndicate.syndicateId
        Task {
            do {
                let updated = try await SyndicateService().updateSyndicate(syndicateId: id, config: config)
                syndicate = updated
                dismiss()
            } catch {
                errorMessage = error.localizedDescription
            }
            isSaving = false
        }
    }
}

#Preview("SheetSyndicateRules - Admin") {
    SheetSyndicateRules(syndicate: .constant(Mock.syndicate), isAdmin: true)
        .environmentObject(AppTheme())
}

#Preview("SheetSyndicateRules - Member") {
    SheetSyndicateRules(syndicate: .constant(Mock.syndicate), isAdmin: false)
        .environmentObject(AppTheme())
}
