import SwiftUI

struct CardBetSlim: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let txn: Txn
    var runner: Runner? = nil
    var showEnhanced: Bool = true

    private var bet: SelectedBet { selectedBet(from: txn) }

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: runner?.symbol ?? "person.fill")
                .font(.caption)
                .foregroundStyle(ProfileOption.color(for: runner?.color ?? ""))
                .frame(width: 16)

            Text(betLabel(for: bet))
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
                .lineLimit(1)

            Spacer(minLength: 8)

            CardPriceUnits(
                price: bet.price,
                unit: bet.unit,
                won: txn.won,
                priceEnhanced: bet.priceEnhanced,
                unitEnhanced: bet.unitEnhanced,
                showEnhanced: showEnhanced,
                priceRowFont: .subheadline.weight(.bold)
            )
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }
}

#Preview("CardBetSlim") {
    VStack(spacing: 8) {
        CardBetSlim(txn: Mock.txnML, runner: Mock.runnerAdmin)
        CardBetSlim(txn: Mock.txnSPR, runner: Mock.runnerMember)
        CardBetSlim(txn: Mock.txnOU, runner: Mock.runnerMember)
    }
    .padding()
    .environmentObject(AppTheme())
}
