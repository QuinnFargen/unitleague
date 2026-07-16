import SwiftUI

struct SheetBookmarks: View {
    @EnvironmentObject private var theme: AppTheme
    @EnvironmentObject private var betStore: BetStore
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("bettorId")            private var bettorId: Int    = 0
    @AppStorage("selectedSyndicateId") private var syndicateId: Int = 0

    @State private var selectedBookmark: PlacedBet?
    @State private var selectedParlayLegs: [PlacedBet]?
    @State private var showingParlay = false

    private var singles: [PlacedBet] {
        betStore.bookmarks.filter { $0.parlayGroupId == nil }
    }

    private var parlayGroups: [(id: UUID, legs: [PlacedBet])] {
        let grouped = Dictionary(grouping: betStore.bookmarks.filter { $0.parlayGroupId != nil }) { $0.parlayGroupId! }
        return grouped.map { (id: $0.key, legs: $0.value) }.sorted { $0.id.uuidString < $1.id.uuidString }
    }

    private var isEmpty: Bool { singles.isEmpty && parlayGroups.isEmpty }

    private func betLabel(for bet: PlacedBet) -> String {
        switch bet.type {
        case "SPR":
            let team = bet.side == "Away" ? bet.awayAbbr : (bet.side == "Home" ? bet.homeAbbr : bet.side)
            if let p = bet.points {
                let s = p == p.rounded()
                    ? (p >= 0 ? "+\(Int(p))" : "\(Int(p))")
                    : String(format: p >= 0 ? "+%.1f" : "%.1f", p)
                return "\(team) \(s)"
            }
            return "\(team) SPR"
        case "O/U":
            if let p = bet.points {
                let s = p == p.rounded() ? "\(Int(p))" : String(format: "%.1f", p)
                return "\(bet.side) \(s)"
            }
            return bet.side.isEmpty ? "O/U" : "\(bet.side) O/U"
        default:
            let team = bet.side == "Away" ? bet.awayAbbr : (bet.side == "Home" ? bet.homeAbbr : bet.side)
            return team.isEmpty ? bet.type : "\(team) \(bet.type)"
        }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isEmpty {
                    Text("No bookmarks")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                } else {
                    ScrollView {
                        VStack(spacing: 10) {
                            // Straight bet bookmarks
                            ForEach(singles) { bookmark in
                                Button {
                                    selectedBookmark = bookmark
                                } label: {
                                    HStack(spacing: 12) {
                                        VStack(alignment: .leading, spacing: 2) {
                                            Text("\(bookmark.awayAbbr) @ \(bookmark.homeAbbr)")
                                                .font(.subheadline.weight(.semibold))
                                                .foregroundStyle(theme.primaryText(colorScheme))
                                            Text(betLabel(for: bookmark))
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        }
                                        Spacer()
                                        HStack(alignment: .firstTextBaseline, spacing: 2) {
                                            Text(String(format: "%.2f", bookmark.price))
                                                .font(.subheadline.weight(.semibold))
                                                .foregroundStyle(theme.accent)
                                            Text("x")
                                                .font(.caption.weight(.semibold))
                                                .foregroundStyle(theme.accent)
                                        }
                                        Button {
                                            betStore.removeBookmark(bookmark)
                                        } label: {
                                            Image(systemName: "bookmark.slash")
                                                .foregroundStyle(theme.error)
                                        }
                                        .buttonStyle(.plain)
                                    }
                                    .padding()
                                    .background(theme.cardBackground(colorScheme))
                                    .clipShape(RoundedRectangle(cornerRadius: 14))
                                }
                                .buttonStyle(.plain)
                            }

                            // Parlay group bookmarks
                            ForEach(parlayGroups, id: \.id) { group in
                                Button {
                                    selectedParlayLegs = group.legs
                                } label: {
                                    VStack(alignment: .leading, spacing: 8) {
                                        HStack {
                                            HStack(spacing: 4) {
                                                Image(systemName: "link")
                                                    .font(.caption.weight(.semibold))
                                                Text("Parlay · \(group.legs.count) legs")
                                                    .font(.caption.weight(.semibold))
                                            }
                                            .foregroundStyle(.secondary)
                                            Spacer()
                                            HStack(alignment: .firstTextBaseline, spacing: 2) {
                                                Text(String(format: "%.2f", group.legs.map(\.price).reduce(1.0, *)))
                                                    .font(.subheadline.weight(.semibold))
                                                    .foregroundStyle(theme.accent)
                                                Text("x")
                                                    .font(.caption.weight(.semibold))
                                                    .foregroundStyle(theme.accent)
                                            }
                                            Button {
                                                betStore.removeBookmarkParlay(groupId: group.id)
                                            } label: {
                                                Image(systemName: "bookmark.slash")
                                                    .foregroundStyle(theme.error)
                                            }
                                            .buttonStyle(.plain)
                                        }

                                        Divider()

                                        ForEach(group.legs) { leg in
                                            HStack {
                                                VStack(alignment: .leading, spacing: 1) {
                                                    Text("\(leg.awayAbbr) @ \(leg.homeAbbr)")
                                                        .font(.caption.weight(.semibold))
                                                        .foregroundStyle(theme.primaryText(colorScheme))
                                                    Text(betLabel(for: leg))
                                                        .font(.caption2)
                                                        .foregroundStyle(.secondary)
                                                }
                                                Spacer()
                                                Text(String(format: "%.2f", leg.price))
                                                    .font(.caption.weight(.semibold))
                                                    .foregroundStyle(theme.accent)
                                            }
                                        }
                                    }
                                    .padding()
                                    .background(theme.cardBackground(colorScheme))
                                    .clipShape(RoundedRectangle(cornerRadius: 14))
                                    .overlay(
                                        RoundedRectangle(cornerRadius: 14)
                                            .strokeBorder(theme.accent.opacity(0.2), lineWidth: 1)
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.horizontal, 16)
                        .padding(.top, 16)
                        .padding(.bottom, 32)
                    }
                }
            }
            .navigationTitle("Bookmarks")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }
                }
                if singles.count >= 2 {
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Parlay") { showingParlay = true }
                            .fontWeight(.semibold)
                            .foregroundStyle(theme.accent)
                    }
                }
            }
            .sheet(item: $selectedBookmark) { bookmark in
                SheetConfirmBet(
                    bet: SelectedBet(placedBet: bookmark),
                    bettorId: bettorId,
                    syndicateId: syndicateId
                )
            }
            .sheet(isPresented: $showingParlay) {
                SheetConfirmParlay(currentBet: nil, bettorId: bettorId, syndicateId: syndicateId)
            }
            .sheet(isPresented: Binding(
                get: { selectedParlayLegs != nil },
                set: { if !$0 { selectedParlayLegs = nil } }
            )) {
                if let legs = selectedParlayLegs {
                    SheetConfirmParlay(
                        currentBet: nil,
                        bettorId: bettorId,
                        syndicateId: syndicateId,
                        savedLegs: legs
                    )
                }
            }
        }
    }
}

#Preview("SheetBookmarks") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetBookmarks()
            .environmentObject(AppTheme())
            .environmentObject(BetStore())
    }
}
