import SwiftUI

/// Shared 3-layer price/units display: plain price×units, then (if present) enhanced
/// price×units, then (if won) the units won in green. Used by `CardBet`'s right-side
/// block and `CardPlacedParlay`'s header.
struct PriceUnitsBlock: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let price: Double
    let unit: Double?
    let won: Bool?
    var priceEnhanced: Double? = nil
    var unitEnhanced: Double? = nil
    var priceRowFont: Font = .headline

    var body: some View {
        VStack(alignment: .trailing, spacing: 3) {
            row(price: price, unit: unit, font: priceRowFont, color: theme.primaryText(colorScheme))

            if let pe = priceEnhanced, let ue = unitEnhanced {
                row(price: pe, unit: ue, font: .caption.weight(.semibold), color: theme.accent)
            }

            if won == true {
                let wonPrice = priceEnhanced ?? price
                let wonUnit = unitEnhanced ?? unit ?? 0
                HStack(alignment: .firstTextBaseline, spacing: 2) {
                    Text("=")
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                    Text(String(format: "%.2f", wonPrice * wonUnit))
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                    Image(systemName: "nairasign.circle.fill")
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                }
            }
        }
    }

    @ViewBuilder
    private func row(price: Double, unit: Double?, font: Font, color: Color) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 2) {
            Text(String(format: "%.2f", price))
                .font(font)
                .foregroundStyle(color)
            if let u = unit {
                Text("x")
                    .font(font)
                    .foregroundStyle(color)
                Text(txnWagerLabel(u))
                    .font(font)
                    .foregroundStyle(color)
                Image(systemName: "nairasign.circle.fill")
                    .font(font)
                    .foregroundStyle(color)
            }
        }
    }
}

#Preview("PriceUnitsBlock") {
    VStack(alignment: .trailing, spacing: 16) {
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil)
        PriceUnitsBlock(price: 1.91, unit: 2, won: false)
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.10, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: true, priceEnhanced: 2.10, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: true)
    }
    .padding()
    .environmentObject(AppTheme())
}
