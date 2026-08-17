import SwiftUI

/// Shared price/units display: "price x units [symbol]". Price and units are each tinted the
/// accent color independently, only when *that* value is actually enhanced (and only while
/// `showEnhanced` is on) — enhancing one doesn't tint the other. On a loss the units value
/// reverts to the base (non-enhanced) amount, since a loss only forfeits the base stake, not
/// the enhancement bonus. Once graded, the symbol turns red on a loss or green on a win; on a
/// win it trails a computed payout ("= price x units") instead of sitting next to the units.
/// The "x" and "=" separators are always plain text, never accent- or win/loss-tinted.
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

    /// A loss only forfeits the base wagered units, not any enhancement bonus, so the
    /// enhanced unit value never applies once the bet is graded a loss.
    private var displayUnit: Double? {
        won == false ? unit : unit.map { effectiveUnitEnhanced ?? $0 }
    }

    private var priceColor: Color {
        effectivePriceEnhanced != nil ? theme.accent : primaryColor
    }

    private var unitColor: Color {
        switch won {
        case false?: return theme.loss
        default:     return effectiveUnitEnhanced != nil ? theme.accent : primaryColor
        }
    }

    private var symbolColor: Color {
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
                    .foregroundStyle(unitColor)
                if won == true {
                    Text("=")
                        .font(priceRowFont)
                        .foregroundStyle(primaryColor)
                    Text(String(format: "%.2f", displayPrice * displayUnit))
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                    Image(systemName: "nairasign.circle.fill")
                        .font(priceRowFont)
                        .foregroundStyle(theme.win)
                } else {
                    Image(systemName: "nairasign.circle.fill")
                        .font(priceRowFont)
                        .foregroundStyle(symbolColor)
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
