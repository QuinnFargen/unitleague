import SwiftUI

struct SheetAddJuice: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    let bettorId: Int
    let syndicateId: Int

    @State private var options: [EnhanceOption] = []
    @State private var leagues: [League] = []
    @State private var isLoading = false
    @State private var isSubmitting = false
    @State private var confirmOption: EnhanceOption? = nil

    private let enhancementService = EnhancementService()
    private let leagueService = LeagueService()

    private var clvOptions: [EnhanceOption]  { options.filter { $0.enhancementType == "clv" } }
    private var teamOptions: [EnhanceOption] { options.filter { $0.enhancementType == "team" } }
    private var edgeOptions: [EnhanceOption] { options.filter { $0.enhancementType == "edge" } }

    private func league(for option: EnhanceOption) -> League? {
        leagues.first { $0.id == option.leagueId }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                } else if options.isEmpty {
                    Text("No enhancements available.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                        .padding(.top, 40)
                } else {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 16) {
                            if !clvOptions.isEmpty {
                                EnhancementGroupHeader("CLV", color: .blue)
                                ForEach(clvOptions) { opt in
                                    Button {
                                        confirmOption = opt
                                    } label: {
                                        CardEnhancement(option: opt, leagueName: league(for: opt)?.abbr)
                                    }
                                    .buttonStyle(.plain)
                                }
                            }

                            if !teamOptions.isEmpty {
                                EnhancementGroupHeader("Team", color: .green)
                                ForEach(teamOptions) { opt in
                                    if let lg = league(for: opt) {
                                        NavigationLink {
                                            ViewTeamList(
                                                league: lg,
                                                presetConf: opt.name == "Conference" ? opt.availableAttrValue : nil,
                                                presetColor: opt.name == "Color" ? opt.availableAttrValue : nil,
                                                presetRegion: opt.name == "Region" ? opt.availableAttrValue : nil,
                                                presetCategory: opt.name == "Mascot" ? opt.availableAttrValue : nil,
                                                pickerTitle: opt.name,
                                                onSelect: { team in
                                                    Task { await submit(opt, teamId: team.id) }
                                                }
                                            )
                                        } label: {
                                            CardEnhancement(option: opt, leagueName: lg.abbr)
                                        }
                                        .buttonStyle(.plain)
                                    }
                                }
                            }

                            if !edgeOptions.isEmpty {
                                EnhancementGroupHeader("Edge", color: .orange)
                                ForEach(edgeOptions) { opt in
                                    Button {
                                        confirmOption = opt
                                    } label: {
                                        CardEnhancement(option: opt, leagueName: league(for: opt)?.abbr)
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                        }
                        .padding(16)
                    }
                }

                if isSubmitting {
                    Color.black.opacity(0.2).ignoresSafeArea()
                    ProgressView()
                }
            }
            .navigationTitle("Add Juice")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .task { await load() }
            .confirmationDialog(
                confirmOption?.name ?? "",
                isPresented: Binding(get: { confirmOption != nil }, set: { if !$0 { confirmOption = nil } }),
                titleVisibility: .visible
            ) {
                Button("Choose Enhancement") {
                    guard let opt = confirmOption else { return }
                    Task { await submit(opt, teamId: 0) }
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

    private func load() async {
        isLoading = true
        defer { isLoading = false }
        async let optsFetch = enhancementService.fetchOptions(bettorId: bettorId, syndicateId: syndicateId)
        async let leaguesFetch = leagueService.fetchLeagues()
        options = (try? await optsFetch) ?? []
        leagues = (try? await leaguesFetch) ?? []
    }

    private func submit(_ option: EnhanceOption, teamId: Int) async {
        isSubmitting = true
        defer { isSubmitting = false }
        guard (try? await enhancementService.chooseEnhancement(
            bettorId: bettorId,
            syndicateId: syndicateId,
            enhancementId: option.enhancementId,
            teamId: teamId,
            level: 1,
            optionHash: option.optionHash
        )) != nil else { return }
        dismiss()
    }
}

// MARK: - Sub-views

struct EnhancementGroupHeader: View {
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

#Preview("SheetAddJuice") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetAddJuice(bettorId: 1, syndicateId: 2)
            .environmentObject(AppTheme())
    }
}
