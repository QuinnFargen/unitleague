import SwiftUI

// MARK: - Helpers

func selectedBet(from txn: Txn) -> SelectedBet {
    SelectedBet(
        betHash:  txn.betHash ?? "",
        type:     txn.betType ?? "",
        side:     "",
        price:    txn.price,
        points:   txn.points,
        awayAbbr: txn.away ?? "—",
        homeAbbr: txn.home ?? "—",
        gameTime: txn.gameTime,
        gameDate: txn.gameDate,
        team:     txn.team,
        unit:     txn.unit
    )
}

func txnWagerLabel(_ units: Double) -> String {
    units == 0.5 ? "½" : String(format: "%.4g", units)
}

// MARK: - CardPlacedBet

struct CardPlacedBet: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let txn: Txn
    var onCancel: (() -> Void)? = nil

    @State private var showCancelConfirm = false

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            if let won = txn.won {
                Circle()
                    .fill(won ? theme.win : theme.loss)
                    .frame(width: 7, height: 7)
                    .padding(.horizontal, 4)
            }
            Button {
                if onCancel != nil { showCancelConfirm = true }
            } label: {
                CardBet(bet: selectedBet(from: txn))
            }
            .buttonStyle(.plain)
            .confirmationDialog("Cancel this bet?", isPresented: $showCancelConfirm, titleVisibility: .visible) {
                Button("Cancel Bet", role: .destructive) { onCancel?() }
            }
        }
    }
}

#Preview("CardPlacedBet") {
    VStack(spacing: 12) {
        CardPlacedBet(txn: Mock.txnML)
        CardPlacedBet(txn: Mock.txnSPR)
        CardPlacedBet(txn: Mock.txnWon)
        CardPlacedBet(txn: Mock.txnLost)
    }
    .padding()
    .environmentObject(AppTheme())
}
