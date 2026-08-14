import SwiftUI

struct CardPlacedParlay: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let legs: [Txn]
    var onCancel: (() -> Void)? = nil
    var isEditing: Bool = false

    @State private var showCancelConfirm = false

    private var combinedOdds: Double {
        legs.map { $0.price ?? 1.0 }.reduce(1.0, *)
    }

    private var won: Bool? {
        if legs.contains(where: { $0.won == false }) { return false }
        if legs.contains(where: { $0.won == nil })    { return nil }
        return true
    }

    private func legBet(_ leg: Txn) -> SelectedBet {
        var bet = selectedBet(from: leg)
        bet.unit = nil
        return bet
    }

    @ViewBuilder
    private var header: some View {
        HStack {
            Text("Parlay")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            Spacer()
            PriceUnitsBlock(
                price: combinedOdds,
                unit: legs.first?.unit,
                won: won,
                priceEnhanced: legs.first?.priceEnhanced,
                unitEnhanced: legs.first?.unitEnhanced,
                priceRowFont: .subheadline.weight(.semibold)
            )
        }
    }

    @ViewBuilder
    private var content: some View {
        VStack(alignment: .leading, spacing: 10) {
            header
            Divider()
            ForEach(legs) { leg in
                if isEditing {
                    CardBet(bet: legBet(leg))
                } else if let gameId = leg.gameId {
                    NavigationLink {
                        ViewGameDetailLoader(gameId: gameId)
                    } label: {
                        CardBet(bet: legBet(leg))
                    }
                    .buttonStyle(.plain)
                } else {
                    CardBet(bet: legBet(leg))
                }
            }
        }
        .padding(14)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .overlay(
            RoundedRectangle(cornerRadius: 14)
                .strokeBorder(theme.divider(colorScheme), lineWidth: 0.5)
        )
    }

    var body: some View {
        Group {
            if isEditing && onCancel != nil {
                Button { showCancelConfirm = true } label: { content }
                    .buttonStyle(.plain)
            } else {
                content
            }
        }
        .confirmationDialog("Cancel this parlay?", isPresented: $showCancelConfirm, titleVisibility: .visible) {
            Button("Cancel Parlay", role: .destructive) { onCancel?() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will cancel your placed parlay and cannot be undone.")
        }
    }
}

#Preview("CardPlacedParlay") {
    CardPlacedParlay(legs: Mock.txnParlay)
        .padding()
        .environmentObject(AppTheme())
}
