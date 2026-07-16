//
//  BetConfirmationSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in SheetConfirmBet & TabProfileView

import SwiftUI

struct SheetConfirmParlay: View {
    @EnvironmentObject private var theme: AppTheme
    @EnvironmentObject private var betStore: BetStore
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("unitBalance") private var unitBalance: Int = 100

    let currentBet: SelectedBet?
    let bettorId: Int
    let syndicateId: Int
    var onSubmit: (() -> Void)? = nil
    var savedLegs: [PlacedBet]? = nil

    // Stable snapshot of legs (UUID-stable for selection tracking)
    @State private var allLegs: [PlacedBet] = []
    @State private var selectedIds: Set<UUID> = []
    @State private var wagerUnits: Double = 1.0

    @State private var runner: Runner?
    @State private var syndicate: Syndicate?
    @State private var localSyndicateId: Int = 0
    @State private var showingSyndicateSelector = false
    @State private var selectorSymbol: String = "house.fill"
    @State private var selectorColorName: String = ""
    @State private var selectorRank: Int = 0

    private let txnService = TxnService()
    private let runnerService = RunnerService()
    private let syndicateService = SyndicateService()

    private var selectedLegs: [PlacedBet] {
        allLegs.filter { selectedIds.contains($0.id) }
    }

    private var combinedOdds: Double {
        selectedLegs.isEmpty ? 1.0 : selectedLegs.map(\.price).reduce(1.0, *)
    }

    private var potentialReturn: Double { wagerUnits * combinedOdds }

    private var impliedPct: String {
        guard combinedOdds > 0 else { return "—" }
        return "\(Int((1.0 / combinedOdds * 100.0).rounded()))%"
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

                        // Leg list with selection toggles
                        VStack(spacing: 10) {
                            ForEach(allLegs) { leg in
                                let isCurrentBet = leg.betHash == currentBet?.betHash
                                let isSelected   = selectedIds.contains(leg.id)

                                Button {
                                    if isSelected { selectedIds.remove(leg.id) }
                                    else          { selectedIds.insert(leg.id) }
                                } label: {
                                    HStack(spacing: 12) {
                                        CardBet(bet: SelectedBet(placedBet: leg))
                                            .overlay(
                                                RoundedRectangle(cornerRadius: 14)
                                                    .strokeBorder(
                                                        isCurrentBet ? theme.accent.opacity(0.6) : .clear,
                                                        lineWidth: 1.5
                                                    )
                                            )

                                        Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                                            .font(.title2)
                                            .foregroundStyle(isSelected ? theme.accent : Color.secondary.opacity(0.4))
                                    }
                                }
                                .buttonStyle(.plain)
                            }
                        }

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
                                    Text(String(format: "%.2f", combinedOdds))
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

                        // Submit Parlay
                        Button {
                            let legs = selectedLegs.map { (betHash: $0.betHash, price: $0.price) }
                            let b = bettorId, s = localSyndicateId, u = wagerUnits
                            let gameDt = selectedLegs.compactMap(\.gameDate).sorted().first
                            Task { try? await txnService.submitParlay(bettorId: b, syndicateId: s, unit: u, legs: legs, gameDt: gameDt) }
                            betStore.clearBookmarks()
                            dismiss()
                            onSubmit?()
                        } label: {
                            HStack(spacing: 6) {
                                Image(systemName: "lock.fill")
                                Text("Submit Parlay")
                            }
                            .font(.body.weight(.semibold))
                            .foregroundStyle(selectedLegs.count >= 2 ? theme.accent : theme.accent.opacity(0.4))
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(theme.accent.opacity(selectedLegs.count >= 2 ? 0.12 : 0.06))
                            .clipShape(RoundedRectangle(cornerRadius: 12))
                        }
                        .disabled(selectedLegs.count < 2)

                        // Bookmark Parlay
                        Button {
                            let parlayLegs = selectedLegs.map { leg in
                                PlacedBet(
                                    betHash: leg.betHash, type: leg.type, side: leg.side,
                                    price: leg.price, points: leg.points, units: wagerUnits,
                                    awayAbbr: leg.awayAbbr, homeAbbr: leg.homeAbbr,
                                    gameTime: leg.gameTime, gameDate: leg.gameDate,
                                    bettorId: bettorId, syndicateId: localSyndicateId
                                )
                            }
                            betStore.bookmarkParlay(parlayLegs)
                            dismiss()
                            onSubmit?()
                        } label: {
                            HStack(spacing: 6) {
                                Image(systemName: "bookmark.fill")
                                Text("Bookmark Parlay")
                            }
                            .font(.body.weight(.semibold))
                            .foregroundStyle(selectedLegs.count >= 2 ? .secondary : Color.secondary.opacity(0.4))
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(Color.secondary.opacity(selectedLegs.count >= 2 ? 0.10 : 0.05))
                            .clipShape(RoundedRectangle(cornerRadius: 12))
                        }
                        .disabled(selectedLegs.count < 2)
                    }
                    .padding(.horizontal, 20)
                    .padding(.vertical, 16)
                }
            }
            .navigationTitle("Confirm Parlay")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
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
            .onAppear {
                localSyndicateId = syndicateId
                if let saved = savedLegs {
                    allLegs = saved
                    selectedIds = Set(saved.map(\.id))
                } else {
                    // Only pull straight-bet bookmarks (no parlay-group legs)
                    var legs = betStore.bookmarks.filter { $0.parlayGroupId == nil }
                    if let cb = currentBet {
                        legs.append(PlacedBet(from: cb, units: 0, bettorId: bettorId, syndicateId: syndicateId))
                    }
                    allLegs = legs
                    selectedIds = Set(legs.map(\.id))
                }
            }
            .task { await fetchIdentity() }
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

#Preview("SheetConfirmParlay – from bookmarks") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetConfirmParlay(
            currentBet: nil,
            bettorId: 42,
            syndicateId: 1,
            savedLegs: Mock.parlayLegs
        )
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
    }
}

#Preview("SheetConfirmParlay – add to existing") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetConfirmParlay(
            currentBet: Mock.selectedBetML,
            bettorId: 42,
            syndicateId: 1
        )
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
    }
}
