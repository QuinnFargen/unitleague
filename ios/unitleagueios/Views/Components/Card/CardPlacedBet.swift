import SwiftUI

// MARK: - Helpers

func selectedBet(from txn: Txn) -> SelectedBet {
    SelectedBet(
        betHash:  txn.betHash ?? "",
        type:     txn.betType ?? "",
        side:     "",
        price:    txn.price ?? 0,
        points:   txn.points,
        awayAbbr: txn.away ?? "—",
        homeAbbr: txn.home ?? "—",
        gameTime: txn.gameTime,
        gameDate: txn.gameDate,
        team:     txn.team,
        unit:     txn.unit,
        unitEnhanced:  txn.unitEnhanced,
        priceEnhanced: txn.priceEnhanced,
        homeScore: txn.homeScore,
        awayScore: txn.awayScore
    )
}

func txnWagerLabel(_ units: Double) -> String {
    units == 0.5 ? "½" : String(format: "%.4g", units)
}

func betLabel(for bet: SelectedBet) -> String {
    let teamSide = bet.team ?? (bet.side == "Away" ? bet.awayAbbr : (bet.side == "Home" ? bet.homeAbbr : bet.side))
    switch bet.type {
    case "SPR":
        if let p = bet.points {
            let s = p == p.rounded()
                ? (p >= 0 ? "+\(Int(p))" : "\(Int(p))")
                : String(format: p >= 0 ? "+%.1f" : "%.1f", p)
            return "\(teamSide) \(s)"
        }
        return "\(teamSide) SPR"
    case "O/U":
        if let p = bet.points {
            let s = p == p.rounded() ? "\(Int(p))" : String(format: "%.1f", p)
            return "\(teamSide) \(s)"
        }
        return "\(teamSide) O/U"
    case "OVER", "UNDER":
        if let p = bet.points {
            let s = p == p.rounded() ? "\(Int(p))" : String(format: "%.1f", p)
            return "\(bet.type) \(s)"
        }
        return bet.type
    default:
        return bet.type.isEmpty ? teamSide : "\(teamSide) \(bet.type)"
    }
}

// MARK: - CardPlacedBet

struct CardPlacedBet: View {
    let txn: Txn
    var onCancel: (() -> Void)? = nil
    var isEditing: Bool = false

    @State private var showCancelConfirm = false

    var body: some View {
        Group {
            if isEditing && onCancel != nil {
                Button { showCancelConfirm = true } label: {
                    CardBet(bet: selectedBet(from: txn), won: txn.won)
                }
                .buttonStyle(.plain)
            } else if let gameId = txn.gameId {
                NavigationLink {
                    ViewGameDetailLoader(gameId: gameId)
                } label: {
                    CardBet(bet: selectedBet(from: txn), won: txn.won)
                }
                .buttonStyle(.plain)
            } else {
                CardBet(bet: selectedBet(from: txn), won: txn.won)
            }
        }
        .confirmationDialog("Cancel this bet?", isPresented: $showCancelConfirm, titleVisibility: .visible) {
            Button("Cancel Bet", role: .destructive) { onCancel?() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will cancel your placed bet and cannot be undone.")
        }
    }
}

#Preview("CardPlacedBet") {
    VStack(spacing: 12) {
        CardPlacedBet(txn: Mock.txnML)
        CardPlacedBet(txn: Mock.txnSPR)
        CardPlacedBet(txn: Mock.txnOU)
        CardPlacedBet(txn: Mock.txnWon)
        CardPlacedBet(txn: Mock.txnLost)
    }
    .padding()
    .environmentObject(AppTheme())
}
