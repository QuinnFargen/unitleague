import SwiftUI

struct CardBetSlim: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let txn: Txn
    var runner: Runner? = nil

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

            HStack(alignment: .firstTextBaseline, spacing: 2) {
                Text(String(format: "%.2f", bet.price))
                    .font(.subheadline.weight(.bold))
                    .foregroundStyle(txn.won == nil ? theme.accent : theme.primaryText(colorScheme))
                Text("x")
                    .font(.caption.weight(.bold))
                    .foregroundStyle(theme.accent)
                if let u = bet.unit {
                    Text(txnWagerLabel(u))
                        .font(.caption.weight(.bold))
                        .foregroundStyle(txn.won == false ? theme.loss : .secondary)
                    Image(systemName: "nairasign.circle.fill")
                        .font(.caption.weight(.bold))
                        .foregroundStyle(txn.won == false ? theme.loss : .secondary)
                }
            }
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
