import SwiftUI

struct CardPlacedUnit: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let txn: Txn

    private var isDeposit: Bool { txn.unit >= 0 }

    var body: some View {
        HStack {
            Image(systemName: isDeposit ? "arrow.up.circle.fill" : "arrow.down.circle.fill")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(isDeposit ? theme.win : theme.loss)
            Text(isDeposit ? "Deposit" : "Withdrawal")
                .foregroundStyle(theme.primaryText(colorScheme))
            Spacer()
            Text(String(format: isDeposit ? "+%.4g" : "%.4g", txn.unit))
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(isDeposit ? theme.win : theme.loss)
        }
        .padding(14)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .overlay(
            RoundedRectangle(cornerRadius: 14)
                .strokeBorder(theme.divider(colorScheme), lineWidth: 0.5)
        )
    }
}

#Preview("CardPlacedUnit") {
    VStack(spacing: 12) {
        CardPlacedUnit(txn: Mock.txnUnitDeposit)
        CardPlacedUnit(txn: Mock.txnUnitWithdrawal)
    }
    .padding()
    .environmentObject(AppTheme())
}
