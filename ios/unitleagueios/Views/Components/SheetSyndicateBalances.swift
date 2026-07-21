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

    let bettorId: Int

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
        txns(for: syndicateId).reduce(0) { $0 + $1.unit * $1.price }
    }

    private func expectedReturn(_ syndicateId: Int) -> Double {
        txns(for: syndicateId).reduce(0) { total, txn in
            guard txn.price > 0 else { return total }
            return total + (1.0 / txn.price) * (txn.unit * txn.price)
        }
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
            .task { await load() }
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
                HStack(spacing: 3) {
                    Text(String(format: "%.4g", runner?.balance ?? 0))
                    Text("units")
                }
                .font(.subheadline.weight(.bold))
                .foregroundStyle(theme.primaryText(colorScheme))
            }
            .font(.subheadline)

            HStack(spacing: 0) {
                metric("Wagered", wagered(syndicate.syndicateId))
                Divider().frame(height: 30)
                metric("Possible", possibleReturn(syndicate.syndicateId))
                Divider().frame(height: 30)
                metric("Expected", expectedReturn(syndicate.syndicateId))
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
        syndicates = raw.filter { seen.insert($0.syndicateId).inserted }
        let runnerList = (try? await runnersTask) ?? []
        runners = Dictionary(runnerList.map { ($0.syndicateId, $0) }, uniquingKeysWith: { first, _ in first })
        activeTxns = (try? await txnsTask) ?? []
    }
}

#Preview("SheetSyndicateBalances") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSyndicateBalances(bettorId: 42)
            .environmentObject(AppTheme())
    }
}
