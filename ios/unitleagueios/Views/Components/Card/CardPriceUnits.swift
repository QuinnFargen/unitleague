import SwiftUI

/// Shared price/units display: "price x units [symbol]". The price is tinted the accent
/// color whenever an enhanced price is in effect, regardless of grading state. Once graded,
/// the units value + symbol turn red on a loss, or green on a win — on a win the symbol moves
/// to trail a computed payout ("= price x units") instead of sitting next to the units.
/// Used by `CardBet`'s right-side block, `CardPlacedParlay`'s header, and `CardBetSlim`.
struct CardPriceUnits: View {
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
        effectivePriceEnhanced != nil ? theme.accent : primaryColor
    }

    private var unitSymbolColor: Color {
        switch won {
        case true?:  return theme.win
        case false?: return theme.loss
        default:     return primaryColor
        }
    }

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 3) {
            Text(String(format: "%.2f", displayPrice))
                .font(priceRowFont)
                .foregroundStyle(priceColor)
            if let displayUnit {
                Text("x")
                    .font(priceRowFont)
                    .foregroundStyle(primaryColor)
                Text(txnWagerLabel(displayUnit))
                    .font(priceRowFont)
                    .foregroundStyle(unitSymbolColor)
                if won == true {
                    Text("= " + String(format: "%.2f", displayPrice * displayUnit))
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                    Image(systemName: "nairasign.circle.fill")
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                } else {
                    Image(systemName: "nairasign.circle.fill")
                        .font(priceRowFont)
                        .foregroundStyle(unitSymbolColor)
                }
            }
        }
    }
}

#Preview("CardPriceUnits") {
    VStack(alignment: .trailing, spacing: 16) {
        CardPriceUnits(price: 1.91, unit: 2, won: nil)
        CardPriceUnits(price: 1.91, unit: 2, won: true)
        CardPriceUnits(price: 1.91, unit: 2, won: false)
        CardPriceUnits(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.35, unitEnhanced: 2.5)
        CardPriceUnits(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.35)
        CardPriceUnits(price: 1.91, unit: 2, won: nil, unitEnhanced: 2.5)
        CardPriceUnits(price: 1.91, unit: 2, won: true, priceEnhanced: 2.35, unitEnhanced: 2.5)
        CardPriceUnits(price: 1.91, unit: 2, won: false, priceEnhanced: 2.35, unitEnhanced: 2.5)
        CardPriceUnits(price: 1.91, unit: 2, won: nil, priceEnhanced: 2.35, unitEnhanced: 2.5, showEnhanced: false)
    }
    .padding()
    .environmentObject(AppTheme())
}
