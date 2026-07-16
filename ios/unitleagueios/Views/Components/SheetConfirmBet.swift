//
//  BetConfirmationSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in ViewGameDetail

import SwiftUI

struct SheetConfirmBet: View {
    @EnvironmentObject private var theme: AppTheme
    @EnvironmentObject private var betStore: BetStore
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("unitBalance") private var unitBalance: Int = 100

    let bet: SelectedBet
    let bettorId: Int
    let syndicateId: Int

    @State private var wagerUnits: Double = 1.0
    @State private var runner: Runner?
    @State private var syndicate: Syndicate?
    @State private var showingParlay = false
    @State private var localSyndicateId: Int = 0
    @State private var showingSyndicateSelector = false
    @State private var selectorSymbol: String = "house.fill"
    @State private var selectorColorName: String = ""
    @State private var selectorRank: Int = 0

    private let runnerService = RunnerService()
    private let syndicateService = SyndicateService()
    private let txnService = TxnService()

    private var potentialReturn: Double { wagerUnits * bet.price }
    private var impliedPct: String {
        guard bet.price > 0 else { return "—" }
        return "\(Int((1.0 / bet.price * 100.0).rounded()))%"
    }

    private func wagerLabel(_ units: Double) -> String {
        units == 0.5 ? "½" : String(format: "%.4g", units)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 16) {

                        CardBet(bet: bet)

                        // Syndicate + Runner identity
                        HStack(spacing: 0) {
                            Button {
                                showingSyndicateSelector = true
                            } label: {
                                HStack(spacing: 8) {
                                    Image(systemName: syndicate?.symbol ?? "house.fill")
                                        .font(.body)
                                        .foregroundStyle(ProfileOption.color(for: syndicate?.color ?? ""))
                                    Text(syndicate?.name ?? "Syndicate")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(theme.primaryText(colorScheme))
                                        .lineLimit(1)
                                    Image(systemName: "chevron.up.chevron.down")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                            }
                            .buttonStyle(.plain)
                            .frame(maxWidth: .infinity, alignment: .leading)

                            Divider().frame(height: 24)

                            HStack(spacing: 8) {
                                Image(systemName: runner?.symbol ?? "person.fill")
                                    .font(.body)
                                    .foregroundStyle(ProfileOption.color(for: runner?.color ?? ""))
                                Text(runner?.profileName ?? "Runner")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .lineLimit(1)
                            }
                            .frame(maxWidth: .infinity, alignment: .trailing)
                        }
                        .padding(.horizontal, 16)
                        .padding(.vertical, 12)
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 14))

                        // Balance
                        HStack {
                            Text("Current Balance")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                            Spacer()
                            Text("\(unitBalance) units")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                        }
                        .padding(.horizontal, 16)
                        .padding(.vertical, 12)
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 14))

                        // Wager stepper
                        HStack(spacing: 0) {
                            Button {
                                wagerUnits = max(0.5, wagerUnits - 0.5)
                            } label: {
                                Image(systemName: "minus.circle.fill")
                                    .font(.largeTitle)
                                    .foregroundStyle(wagerUnits <= 0.5 ? theme.loss.opacity(0.3) : theme.loss)
                            }
                            .buttonStyle(.plain)
                            .disabled(wagerUnits <= 0.5)

                            Spacer()

                            VStack(spacing: 2) {
                                Text(wagerLabel(wagerUnits))
                                    .font(.title2.weight(.bold))
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                HStack(spacing: 3) {
                                    Text("Units")
                                    Image(systemName: "nairasign.circle.fill")
                                }
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(.secondary)
                            }

                            Spacer()

                            Button {
                                wagerUnits += 0.5
                            } label: {
                                Image(systemName: "plus.circle.fill")
                                    .font(.largeTitle)
                                    .foregroundStyle(theme.win)
                            }
                            .buttonStyle(.plain)
                        }
                        .padding(.horizontal, 16)
                        .padding(.vertical, 12)
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 14))

                        // Summary banner
                        HStack(spacing: 0) {
                            summaryCell("Risked") {
                                HStack(spacing: 3) {
                                    Text(wagerLabel(wagerUnits))
                                    Image(systemName: "nairasign.circle.fill")
                                }
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            }
                            Divider().frame(height: 36)
                            summaryCell("Price") {
                                HStack(alignment: .firstTextBaseline, spacing: 2) {
                                    Text(String(format: "%.2f", bet.price))
                                    Text("x").font(.caption.weight(.semibold))
                                }
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(theme.accent)
                            }
                            Divider().frame(height: 36)
                            summaryCell("Return") {
                                HStack(spacing: 3) {
                                    Text(wagerLabel(potentialReturn))
                                    Image(systemName: "nairasign.circle.fill")
                                }
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(theme.primaryText(colorScheme))
                            }
                            Divider().frame(height: 36)
                            summaryCell("Implied") {
                                Text(impliedPct)
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(.secondary)
                            }
                        }
                        .padding(.vertical, 14)
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 14))

                        // Submit
                        Button {
                            let b = bettorId, s = localSyndicateId, h = bet.betHash, u = wagerUnits, p = bet.price, d = bet.gameDate
                            Task { try? await txnService.submitBet(bettorId: b, syndicateId: s, betHash: h, unit: u, price: p, gameDt: d) }
                            dismiss()
                        } label: {
                            HStack(spacing: 6) {
                                Image(systemName: "lock.fill")
                                Text("Submit Bet")
                            }
                            .font(.body.weight(.semibold))
                            .foregroundStyle(theme.accent)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(theme.accent.opacity(0.12))
                            .clipShape(RoundedRectangle(cornerRadius: 12))
                        }

                        // Bookmark
                        Button {
                            betStore.bookmark(PlacedBet(from: bet, units: wagerUnits, bettorId: bettorId, syndicateId: localSyndicateId))
                            dismiss()
                        } label: {
                            HStack(spacing: 6) {
                                Image(systemName: "bookmark.fill")
                                Text("Bookmark")
                            }
                            .font(.body.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(Color.secondary.opacity(0.10))
                            .clipShape(RoundedRectangle(cornerRadius: 12))
                        }
                    }
                    .padding(.horizontal, 20)
                    .padding(.vertical, 16)
                }
            }
            .navigationTitle("Confirm Bet")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                if betStore.bookmarks.contains(where: { $0.parlayGroupId == nil }) {
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Parlay") { showingParlay = true }
                            .fontWeight(.semibold)
                            .foregroundStyle(theme.accent)
                    }
                }
            }
            .sheet(isPresented: $showingParlay) {
                SheetConfirmParlay(
                    currentBet: bet,
                    bettorId: bettorId,
                    syndicateId: localSyndicateId,
                    onSubmit: { dismiss() }
                )
            }
            .sheet(isPresented: $showingSyndicateSelector) {
                SheetSyndicateSelector(
                    bettorId: bettorId,
                    selectedSyndicateId: $localSyndicateId,
                    leagueSymbol: $selectorSymbol,
                    leagueColorName: $selectorColorName,
                    leagueRank: $selectorRank
                )
            }
            .task {
                localSyndicateId = syndicateId
                await fetchIdentity()
            }
            .onChange(of: localSyndicateId) {
                Task { await fetchIdentity() }
            }
        }
    }

    private func fetchIdentity() async {
        let sid = localSyndicateId
        async let runnerTask    = try? runnerService.fetchRunner(bettorId: bettorId, syndicateId: sid)
        async let syndicateTask = try? syndicateService.fetchSyndicate(syndicateId: sid, bettorId: nil)
        let (runners, syndicates) = await (runnerTask, syndicateTask)
        runner    = runners?.first
        syndicate = syndicates?.first
    }

    @ViewBuilder
    private func summaryCell(_ label: String, @ViewBuilder value: () -> some View) -> some View {
        VStack(spacing: 4) {
            value()
            Text(label)
                .font(.caption2)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
    }
}

#Preview("SheetConfirmBet – ML") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetConfirmBet(
            bet: Mock.selectedBetML,
            bettorId: 42,
            syndicateId: 1
        )
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
    }
}

#Preview("SheetConfirmBet – SPR") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetConfirmBet(
            bet: Mock.selectedBetSPR,
            bettorId: 42,
            syndicateId: 1
        )
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
    }
}
