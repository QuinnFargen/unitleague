import SwiftUI

struct SheetAddJuice: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    let bettorId: Int
    let syndicates: [Syndicate]

    @State private var options: [EnhanceOption] = []
    @State private var leagues: [League] = []
    @State private var myRunners: [Int: Runner] = [:]
    @State private var isLoading = false
    @State private var isSubmitting = false
    @State private var confirmOption: EnhanceOption? = nil

    private let enhancementService = EnhancementService()
    private let leagueService = LeagueService()
    private let runnerService = RunnerService()

    private func syndicateOptions(_ syndicateId: Int) -> [EnhanceOption] {
        options.filter { $0.syndicateId == syndicateId }
    }

    private func league(for option: EnhanceOption) -> League? {
        leagues.first { $0.id == option.leagueId }
    }

    var body: some View {
        NavigationStack {
            addJuiceContent
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

    private var addJuiceContent: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            Group {
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
                        VStack(alignment: .leading, spacing: 20) {
                            ForEach(syndicates) { syn in
                                let synOptions = syndicateOptions(syn.syndicateId)
                                if !synOptions.isEmpty {
                                    let clvOptions = synOptions.filter { $0.enhancementType == "clv" }
                                    let teamOptions = synOptions.filter { $0.enhancementType == "team" }
                                    let edgeOptions = synOptions.filter { $0.enhancementType == "edge" }

                                    VStack(alignment: .leading, spacing: 16) {
                                        syndicateHeader(syndicate: syn)

                                        if !clvOptions.isEmpty {
                                            EnhancementGroupHeader("CLV", color: .green)
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
                                            EnhancementGroupHeader("Team", color: .yellow)
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
                                            EnhancementGroupHeader("Edge", color: .red)
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
                                    .padding(.bottom, 4)
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
        }
    }

    @ViewBuilder
    private func syndicateHeader(syndicate: Syndicate) -> some View {
        HStack(spacing: 6) {
            Image(systemName: syndicate.symbol ?? "house.fill")
                .font(.caption.weight(.semibold))
                .foregroundStyle(ProfileOption.color(for: syndicate.color ?? ""))
            Text(syndicate.name)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            if let runner = myRunners[syndicate.syndicateId] {
                Text("-")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Image(systemName: runner.symbol ?? "person.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Text(runner.profileName ?? "Runner \(bettorId)")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 4)
    }

    private func load() async {
        isLoading = true
        defer { isLoading = false }
        async let leaguesFetch = leagueService.fetchLeagues()
        async let runnersFetch = runnerService.fetchRunner(bettorId: bettorId)

        var combined: [EnhanceOption] = []
        for syn in syndicates {
            if let opts = try? await enhancementService.fetchOptions(bettorId: bettorId, syndicateId: syn.syndicateId) {
                combined.append(contentsOf: opts)
            }
        }
        options = combined
        leagues = (try? await leaguesFetch) ?? []

        let fetchedRunners = (try? await runnersFetch) ?? []
        var map: [Int: Runner] = [:]
        for runner in fetchedRunners where runner.bettorId == bettorId {
            map[runner.syndicateId] = runner
        }
        myRunners = map
    }

    private func submit(_ option: EnhanceOption, teamId: Int) async {
        isSubmitting = true
        defer { isSubmitting = false }
        guard (try? await enhancementService.chooseEnhancement(
            bettorId: bettorId,
            syndicateId: option.syndicateId,
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
        SheetAddJuice(bettorId: 1, syndicates: Mock.syndicates)
            .environmentObject(AppTheme())
    }
}
