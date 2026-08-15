import SwiftUI

/// Shared price/units display. Shows the enhanced price and/or units (whichever exist) in the
/// accent color while the bet is pending; once graded, colors reset to plain except the unit
/// value + currency icon turn red on a loss, or green on a win with a trailing "= Won" label.
/// Used by `CardBet`'s right-side block, `CardPlacedParlay`'s header, and `CardBetSlim`.
struct PriceUnitsBlock: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let price: Double
    let unit: Double?
    let won: Bool?
    var priceEnhanced: Double? = nil
    var unitEnhanced: Double? = nil
    var showEnhanced: Bool = true
    var priceRowFont: Font = .headline

    private var effectivePriceEnhanced: Double? { showEnhanced ? priceEnhanced : nil }
    private var effectiveUnitEnhanced: Double? { showEnhanced ? unitEnhanced : nil }

    private var primaryColor: Color { theme.primaryText(colorScheme) }

    private var displayPrice: Double { effectivePriceEnhanced ?? price }
    private var displayUnit: Double? { unit.map { effectiveUnitEnhanced ?? $0 } }

    private var priceColor: Color {
        won == nil && effectivePriceEnhanced != nil ? theme.accent : primaryColor
    }

    private var unitSymbolColor: Color {
        switch won {
        case true?:  return theme.win
        case false?: return theme.loss
        default:     return (effectiveUnitEnhanced != nil && unit != nil) ? theme.accent : primaryColor
        }
    }

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 3) {
            Text(String(format: "%.2f", displayPrice))
                .font(priceRowFont)
                .foregroundStyle(priceColor)
            if let displayUnit {
                Text(txnWagerLabel(displayUnit))
                    .font(priceRowFont)
                    .foregroundStyle(unitSymbolColor)
                Image(systemName: "nairasign.circle.fill")
                    .font(priceRowFont)
                    .foregroundStyle(unitSymbolColor)
                if won == true {
                    Text("= Won")
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
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
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.35)
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: true, priceEnhanced: 2.35, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: false, priceEnhanced: 2.35, unitEnhanced: 2.5)
        PriceUnitsBlock(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.35, unitEnhanced: 2.5, showEnhanced: false)
    }
    .padding()
    .environmentObject(AppTheme())
}
