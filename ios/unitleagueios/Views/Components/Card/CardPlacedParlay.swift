import SwiftUI

struct CardPlacedParlay: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let legs: [Txn]
    var onCancel: (() -> Void)? = nil

    @State private var showCancelConfirm = false

    private var combinedOdds: Double {
        legs.map(\.price).reduce(1.0, *)
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

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Parlay")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Spacer()
                HStack(alignment: .firstTextBaseline, spacing: 2) {
                    Text(String(format: "%.2f", combinedOdds))
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.accent)
                    Text("x")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.accent)
                    Text(txnWagerLabel(legs.first?.unit ?? 0))
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(won == false ? theme.loss : .secondary)
                    Image(systemName: "nairasign.circle.fill")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(won == false ? theme.loss : .secondary)
                    if won == true {
                        Text("=")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.win)
                        Text(String(format: "%.2f", combinedOdds * (legs.first?.unit ?? 0)))
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.win)
                    }
                }
            }

            Divider()

            ForEach(legs) { leg in
                CardBet(bet: legBet(leg))
            }
        }
        .padding(14)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .overlay(
            RoundedRectangle(cornerRadius: 14)
                .strokeBorder(theme.divider(colorScheme), lineWidth: 0.5)
        )
        .onLongPressGesture {
            if onCancel != nil { showCancelConfirm = true }
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
