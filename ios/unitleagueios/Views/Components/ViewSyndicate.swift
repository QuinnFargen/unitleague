import SwiftUI

struct ViewSyndicate: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId")            private var bettorId: Int = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int = 0
    @AppStorage("leagueSymbol")        private var leagueSymbol: String = "person.circle.fill"
    @AppStorage("leagueColorName")     private var leagueColorName: String = AccentOption.allCases[0].rawValue
    @AppStorage("leagueRank")          private var leagueRank: Int = 0

    @State var syndicate: Syndicate
    let onJoined: (() -> Void)? = nil
    @State private var runners: [Runner] = []
    @State private var activeBets: [Txn] = []
    @State private var isLoading = false
    @State private var fetchError: String?
    @State private var showingEdit = false
    @State private var showingStartConfirm = false
    @State private var showingRules = false
    @State private var showingRunnerEdit = false
    @State private var isStarting = false
    @State private var startError: String?
    @State private var isJoiningPublic = false
    @State private var joinPublicError: String?

    private var currentRunner: Runner? { runners.first(where: { $0.bettorId == bettorId }) }
    private var isAdmin: Bool { currentRunner?.role == "admin" }
    private var canJoinPublic: Bool { currentRunner == nil && syndicate.isPublic && !syndicate.isStarted }

    private var currentRunnerBinding: Binding<Runner>? {
        guard let idx = runners.firstIndex(where: { $0.bettorId == bettorId }) else { return nil }
        return $runners[idx]
    }

    private var sortedRunners: [Runner] {
        runners.sorted { ($0.balance ?? 0) > ($1.balance ?? 0) }
    }

    private func ordinal(_ n: Int) -> String {
        switch n {
        case 1:  return "1st"
        case 2:  return "2nd"
        case 3:  return "3rd"
        default: return "\(n)th"
        }
    }

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            ScrollView {
                VStack(spacing: 20) {
                    syndicateBanner

                    if isLoading {
                        ProgressView()
                            .frame(maxWidth: .infinity)
                            .padding(.top, 20)
                    } else if let error = fetchError {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .padding(.top, 20)
                    } else if !sortedRunners.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Standing")
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 4)

                            VStack(spacing: 0) {
                                ForEach(Array(sortedRunners.enumerated()), id: \.element.id) { index, runner in
                                    RunnerRow(
                                        rank: index + 1,
                                        runner: runner,
                                        isCurrentUser: runner.bettorId == bettorId,
                                        ordinal: ordinal
                                    )
                                    if index < sortedRunners.count - 1 {
                                        Divider().padding(.horizontal, 16)
                                    }
                                }
                            }
                            .background(theme.cardBackground(colorScheme))
                            .clipShape(RoundedRectangle(cornerRadius: 14))
                        }
                        .padding(.horizontal, 16)
                    }

                    if !activeBets.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Bets")
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 4)

                            VStack(spacing: 8) {
                                ForEach(activeBets) { txn in
                                    CardBetSlim(txn: txn, runner: runners.first(where: { $0.bettorId == txn.bettorId }))
                                }
                            }
                        }
                        .padding(.horizontal, 16)
                    }
                }
                .padding(.bottom, 32)
            }
        }
        .navigationTitle("")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                HStack(spacing: 8) {
                    Button {
                        showingRules = true
                    } label: {
                        Text("Rules")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                            .padding(.horizontal, 12)
                            .padding(.vertical, 6)
                            .background(theme.cardBackgroundProminent(colorScheme))
                            .clipShape(Capsule())
                    }

                    if currentRunner != nil {
                        Button {
                            showingRunnerEdit = true
                        } label: {
                            Text("Runner")
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                                .padding(.horizontal, 12)
                                .padding(.vertical, 6)
                                .background(theme.cardBackgroundProminent(colorScheme))
                                .clipShape(Capsule())
                        }

                        let isSelected = selectedSyndicateId == syndicate.syndicateId
                        Button {
                            if isSelected {
                                selectedSyndicateId = 0
                                leagueSymbol = "person.circle.fill"
                                leagueColorName = AccentOption.allCases[0].rawValue
                                leagueRank = 0
                            } else {
                                selectedSyndicateId = syndicate.syndicateId
                                leagueSymbol = syndicate.symbol ?? "person.3.fill"
                                leagueColorName = syndicate.color ?? AccentOption.allCases[0].rawValue
                                leagueRank = rankInSyndicate()
                            }
                        } label: {
                            Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                                .font(.title3)
                                .foregroundStyle(isSelected ? theme.accent : .secondary)
                        }
                    }
                }
            }
        }
        .task { await load() }
        .sheet(isPresented: $showingEdit) {
            SheetSyndicateEdit(syndicate: $syndicate)
        }
        .sheet(isPresented: $showingRules) {
            SheetSyndicateRules(syndicate: $syndicate, isAdmin: isAdmin)
        }
        .sheet(isPresented: $showingRunnerEdit) {
            if let binding = currentRunnerBinding {
                SheetEditProfile(runner: binding)
            }
        }
        .confirmationDialog(
            "Start the league?",
            isPresented: $showingStartConfirm,
            titleVisibility: .visible
        ) {
            Button("Start League", role: .destructive) {
                Task { await startLeague() }
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            let units = syndicate.startUnits ?? 0
            Text("This will lock in all \(runners.count) members and deduct \(units) units each.")
        }
    }

    private func startLeague() async {
        isStarting = true
        startError = nil
        do {
            syndicate = try await SyndicateService().startSyndicate(syndicateId: syndicate.syndicateId, bettorId: bettorId)
        } catch {
            startError = error.localizedDescription
        }
        isStarting = false
    }

    private func joinLeague() async {
        guard let code = syndicate.code else {
            joinPublicError = "Missing syndicate code."
            return
        }
        isJoiningPublic = true
        joinPublicError = nil
        do {
            _ = try await SyndicateService().joinSyndicate(bettorId: bettorId, code: code)
            if let onJoined {
                onJoined()
            } else {
                await load()
            }
        } catch {
            joinPublicError = error.localizedDescription
        }
        isJoiningPublic = false
    }

    private var syndicateBanner: some View {
        let bannerColor = ProfileOption.color(for: syndicate.color ?? "")
        let iconName = syndicate.symbol ?? (syndicate.isPublic ? "sparkles" : "person.3.fill")

        return VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .center, spacing: 14) {
                Image(systemName: iconName)
                    .font(.system(size: 28, weight: .semibold))
                    .foregroundStyle(bannerColor)
                    .frame(width: 48, height: 48)
                    .background(theme.cardBackgroundProminent(colorScheme))
                    .clipShape(RoundedRectangle(cornerRadius: 12))

                VStack(alignment: .leading, spacing: 3) {
                    Text(syndicate.name)
                        .font(.title2).bold()
                        .foregroundStyle(theme.primaryText(colorScheme))

                    HStack(spacing: 8) {
                        if syndicate.isPublic {
                            Text("Public")
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(theme.accent)
                        }
                        Text("\(runners.count) members")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)
                        if let su = syndicate.startUnits, su > 0 {
                            Text("\(su) units")
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(.secondary)
                        }
                    }
                }

                Spacer()

                if isAdmin {
                    Button {
                        showingEdit = true
                    } label: {
                        Image(systemName: "pencil.circle")
                            .font(.title3)
                            .foregroundStyle(theme.accent)
                    }
                }
            }

            if isAdmin && !syndicate.isStarted {
                if let err = startError {
                    Text(err)
                        .font(.caption)
                        .foregroundStyle(.red)
                }
                Button {
                    showingStartConfirm = true
                } label: {
                    HStack(spacing: 6) {
                        if isStarting {
                            ProgressView().scaleEffect(0.7)
                        }
                        Text("Start League")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.accent)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 10)
                    .background(theme.accent.opacity(0.12))
                    .clipShape(Capsule())
                }
                .disabled(isStarting)
            }

            if canJoinPublic {
                if let err = joinPublicError {
                    Text(err)
                        .font(.caption)
                        .foregroundStyle(.red)
                }
                Button {
                    Task { await joinLeague() }
                } label: {
                    HStack(spacing: 6) {
                        if isJoiningPublic {
                            ProgressView().scaleEffect(0.7)
                        }
                        Text("Join this league")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.accent)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 10)
                    .background(theme.accent.opacity(0.12))
                    .clipShape(Capsule())
                }
                .disabled(isJoiningPublic)
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .padding(.horizontal, 16)
        .padding(.top, 16)
    }

    private func rankInSyndicate() -> Int {
        let sorted = runners.sorted { ($0.balance ?? 0) > ($1.balance ?? 0) }
        if let idx = sorted.firstIndex(where: { $0.bettorId == bettorId }) {
            return idx + 1
        }
        return 0
    }

    private func load() async {
        isLoading = true
        fetchError = nil
        do {
            async let runnersTask = RunnerService().fetchRunner(syndicateId: syndicate.syndicateId)
            async let betsTask = TxnService().fetchActiveBets(syndicateId: syndicate.syndicateId)
            runners = try await runnersTask
            activeBets = (try? await betsTask) ?? []
            if selectedSyndicateId == syndicate.syndicateId {
                leagueRank = rankInSyndicate()
            }
        } catch {
            fetchError = error.localizedDescription
        }
        isLoading = false
    }
}

// MARK: - RunnerRow

private struct RunnerRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let rank: Int
    let runner: Runner
    let isCurrentUser: Bool
    let ordinal: (Int) -> String

    private func badge(_ text: String, prominent: Bool) -> some View {
        Text(text)
            .font(.caption2.weight(.semibold))
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .foregroundStyle(prominent ? theme.accent : .secondary)
            .background(prominent ? theme.accent.opacity(0.15) : theme.cardBackgroundProminent(colorScheme))
            .clipShape(Capsule())
    }

    var body: some View {
        HStack(spacing: 14) {
            Text(ordinal(rank))
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 28, alignment: .leading)

            Image(systemName: runner.symbol ?? "person.fill")
                .font(.title2)
                .foregroundStyle(ProfileOption.color(for: runner.color ?? ""))

            VStack(alignment: .leading, spacing: 2) {
                Text(runner.profileName ?? "Unknown")
                    .font(.body).fontWeight(isCurrentUser ? .semibold : .regular)
                    .foregroundStyle(theme.primaryText(colorScheme))

                if runner.role == "admin" || isCurrentUser {
                    HStack(spacing: 4) {
                        if runner.role == "admin" { badge("admin", prominent: true) }
                        if isCurrentUser { badge("you", prominent: runner.role != "admin") }
                    }
                }
            }

            Spacer()

            HStack(spacing: 3) {
                Image(systemName: "nairasign.circle.fill")
                Text(txnWagerLabel(runner.balance ?? 0)).fontWeight(.semibold)
            }
            .font(.subheadline)
            .foregroundStyle(theme.primaryText(colorScheme))
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(isCurrentUser ? theme.cardBackgroundProminent(colorScheme) : Color.clear)
    }
}

#Preview("ViewSyndicate") {
    NavigationStack {
        ViewSyndicate(syndicate: Mock.syndicate)
    }
    .environmentObject(AppTheme())
}
