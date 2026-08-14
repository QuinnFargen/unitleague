import SwiftUI

/// Shared price/units display: a single line of price×units, with the enhanced values
/// (when present) shown at the primary size in the accent color and the plain values
/// shown smaller alongside them. The trailing units symbol recolors green/red once the
/// bet is won/lost. Used by `CardBet`'s right-side block and `CardPlacedParlay`'s header.
struct PriceUnitsBlock: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let price: Double
    let unit: Double?
    let won: Bool?
    var priceEnhanced: Double? = nil
    var unitEnhanced: Double? = nil
    var priceRowFont: Font = .headline

    private var hasEnhanced: Bool {
        priceEnhanced != nil && unitEnhanced != nil && unit != nil
    }

    private var primaryColor: Color { theme.primaryText(colorScheme) }

    private var iconColor: Color {
        switch won {
        case true?:  return theme.win
        case false?: return theme.loss
        default:     return primaryColor
        }
    }

    var body: some View {
        if hasEnhanced, let pe = priceEnhanced, let ue = unitEnhanced, let u = unit {
            HStack(alignment: .firstTextBaseline, spacing: 3) {
                Text(String(format: "%.2fx", pe))
                    .font(priceRowFont)
                    .foregroundStyle(theme.accent)
                Text("(\(String(format: "%.2f", price)))")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Text("x")
                    .font(priceRowFont)
                    .foregroundStyle(theme.accent)
                Text(txnWagerLabel(ue))
                    .font(priceRowFont)
                    .foregroundStyle(theme.accent)
                Text("(\(txnWagerLabel(u)))")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Image(systemName: "nairasign.circle.fill")
                    .font(priceRowFont)
                    .foregroundStyle(iconColor)
            }
        } else {
            HStack(alignment: .firstTextBaseline, spacing: 3) {
                Text(String(format: "%.2fx", price))
                    .font(priceRowFont)
                    .foregroundStyle(primaryColor)
                if let u = unit {
                    Text("x")
                        .font(priceRowFont)
                        .foregroundStyle(primaryColor)
                    Text(txnWagerLabel(u))
                        .font(priceRowFont)
                        .foregroundStyle(primaryColor)
                    Image(systemName: "nairasign.circle.fill")
                        .font(priceRowFont)
                        .foregroundStyle(iconColor)
                }
            }
        }
    }
}

#Preview("PriceUnitsBlock") {
    VStack(alignment: .trailing, spacing: 16) {
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil)
        PriceUnitsBlock(price: 1.91, unit: 2, won: true)
        PriceUnitsBlock(price: 1.91, unit: 2, won: false)
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.35, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: true, priceEnhanced: 2.35, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: false, priceEnhanced: 2.35, unitEnhanced: 2.5)
    }
    .padding()
    .environmentObject(AppTheme())
}
