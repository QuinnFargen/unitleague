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

    var body: some View {
        Button {
            if onCancel != nil { showCancelConfirm = true }
        } label: {
            VStack(alignment: .leading, spacing: 10) {
                HStack {
                    Text("Parlay")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Spacer()
                    if let won = legs.first?.won {
                        Circle()
                            .fill(won ? theme.win : theme.loss)
                            .frame(width: 7, height: 7)
                    }
                    HStack(alignment: .firstTextBaseline, spacing: 2) {
                        Text(String(format: "%.2f", combinedOdds))
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.accent)
                        Text("x")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.accent)
                        Text(txnWagerLabel(legs.first?.unit ?? 0))
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.secondary)
                        Image(systemName: "nairasign.circle.fill")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.secondary)
                    }
                }

                Divider()

                ForEach(legs) { leg in
                    CardBet(bet: selectedBet(from: leg))
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
        .buttonStyle(.plain)
        .confirmationDialog("Cancel this parlay?", isPresented: $showCancelConfirm, titleVisibility: .visible) {
            Button("Cancel Parlay", role: .destructive) { onCancel?() }
        }
    }
}

#Preview("CardPlacedParlay") {
    CardPlacedParlay(legs: Mock.txnParlay)
        .padding()
        .environmentObject(AppTheme())
}
