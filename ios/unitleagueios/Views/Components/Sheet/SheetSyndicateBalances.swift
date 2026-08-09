//
//  SheetSyndicateBalances.swift
//  unitleagueios
//
//  Used in SharedToolbar

import SwiftUI

struct SheetSyndicateBalances: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("profileSymbol") private var profileSymbol: String = ProfileOption.symbols[0]
    @AppStorage("userUnits")     private var userUnits: Int         = 100

    let bettorId: Int
    var mockData: (syndicates: [Syndicate], runners: [Int: Runner], txns: [Txn])? = nil

    @State private var syndicates: [Syndicate] = []
    @State private var runners: [Int: Runner] = [:]
    @State private var activeTxns: [Txn] = []
    @State private var isLoading = false

    private let syndicateService = SyndicateService()
    private let runnerService = RunnerService()
    private let txnService = TxnService()

    private func txns(for syndicateId: Int) -> [Txn] {
        activeTxns.filter { $0.syndicateId == syndicateId && $0.canceled != true }
    }

    private func wagered(_ syndicateId: Int) -> Double {
        txns(for: syndicateId).reduce(0) { $0 + $1.unit }
    }

    private func possibleReturn(_ syndicateId: Int) -> Double {
        txns(for: syndicateId).reduce(0) { $0 + $1.unit * ($1.price ?? 0) }
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

    private var soloRunner: Runner {
        Runner(
            runnerId: 0,
            bettorId: bettorId,
            syndicateId: 0,
            role: "solo",
            active: true,
            balance: Double(userUnits),
            profileName: nil,
            symbol: profileSymbol,
            color: theme.accentOption.rawValue
        )
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                } else if syndicates.isEmpty {
                    Text("No syndicates found.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                } else {
                    ScrollView {
                        VStack(spacing: 12) {
                            ForEach(syndicates) { syndicate in
                                syndicateRow(syndicate)
                            }
                        }
                        .padding(16)
                    }
                }
            }
            .navigationTitle("Balances")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .task {
                if let mockData {
                    syndicates = mockData.syndicates
                    runners = mockData.runners
                    activeTxns = mockData.txns
                } else {
                    await load()
                }
            }
        }
    }

    @ViewBuilder
    private func syndicateRow(_ syndicate: Syndicate) -> some View {
        let runner = runners[syndicate.syndicateId]
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 6) {
                Image(systemName: syndicate.symbol ?? "house.fill")
                    .foregroundStyle(ProfileOption.color(for: syndicate.color ?? ""))
                Text(syndicate.name)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let runner {
                    Text("-")
                        .foregroundStyle(.secondary)
                    Image(systemName: runner.symbol ?? "person.fill")
                        .foregroundStyle(.secondary)
                    Text(runner.profileName ?? "")
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }
            .font(.subheadline)

            HStack(spacing: 0) {
                metric("Wagered", wagered(syndicate.syndicateId))
                Divider().frame(height: 30)
                metric("Possible", possibleReturn(syndicate.syndicateId))
                Divider().frame(height: 30)
                metric("Balance", runner?.balance ?? 0)
            }
        }
        .padding(14)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    @ViewBuilder
    private func metric(_ label: String, _ value: Double) -> some View {
        VStack(spacing: 2) {
            Text(String(format: "%.4g", value))
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            Text(label)
                .font(.caption2)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
    }

    private func load() async {
        guard bettorId > 0 else { return }
        isLoading = true
        defer { isLoading = false }
        async let syndicatesTask = syndicateService.fetchSyndicate(bettorId: bettorId)
        async let runnersTask = runnerService.fetchRunner(bettorId: bettorId)
        async let txnsTask = txnService.fetchActiveBets(bettorId: bettorId)
        let raw = (try? await syndicatesTask) ?? []
        var seen = Set<Int>()
        syndicates = [soloSyndicate] + raw.filter { seen.insert($0.syndicateId).inserted }
        let runnerList = (try? await runnersTask) ?? []
        var runnerMap = Dictionary(runnerList.map { ($0.syndicateId, $0) }, uniquingKeysWith: { first, _ in first })
        runnerMap[0] = soloRunner
        runners = runnerMap
        activeTxns = (try? await txnsTask) ?? []
    }
}

#Preview("SheetSyndicateBalances") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSyndicateBalances(
            bettorId: 42,
            mockData: (
                syndicates: Mock.syndicates,
                runners: Dictionary(Mock.runners.map { ($0.syndicateId, $0) }, uniquingKeysWith: { first, _ in first }),
                txns: [Mock.txnML, Mock.txnSPR, Mock.txnOU]
            )
        )
        .environmentObject(AppTheme())
    }
}
