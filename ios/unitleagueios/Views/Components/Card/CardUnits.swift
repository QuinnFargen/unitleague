import SwiftUI

struct CardUnits: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let txn: Txn
    var syndicate: Syndicate? = nil

    private var isDeposit: Bool { txn.unit >= 0 }

    var body: some View {
        HStack {
            Image(systemName: isDeposit ? "arrow.up.circle.fill" : "arrow.down.circle.fill")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(isDeposit ? theme.win : theme.loss)

            if isDeposit {
                Text("Initial Units")
                    .foregroundStyle(theme.primaryText(colorScheme))
            } else {
                HStack(spacing: 4) {
                    Text("Buy In:")
                    Image(systemName: syndicate?.symbol ?? "house.fill")
                    Text(syndicate?.name ?? "Syndicate")
                }
                .foregroundStyle(theme.primaryText(colorScheme))
            }

            Spacer()

            HStack(alignment: .firstTextBaseline, spacing: 2) {
                Text(String(format: isDeposit ? "+%.4g" : "%.4g", txn.unit))
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(isDeposit ? theme.win : theme.loss)
                Image(systemName: "nairasign.circle.fill")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(isDeposit ? theme.win : theme.loss)
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
}

#Preview("CardUnits") {
    VStack(spacing: 12) {
        CardUnits(txn: Mock.txnUnitDeposit)
        CardUnits(txn: Mock.txnUnitWithdrawal, syndicate: Mock.syndicate)
    }
    .padding()
    .environmentObject(AppTheme())
}
