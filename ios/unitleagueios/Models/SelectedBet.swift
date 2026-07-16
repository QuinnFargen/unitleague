import Foundation

struct SelectedBet: Identifiable {
    let id = UUID()
    let betHash: String
    let type: String        // "ML", "SPR", "O/U", or "" when unknown
    let side: String        // kept for PlacedBet compat; "" when unused
    let price: Double
    let points: Double?     // spread value or O/U total; nil for ML
    let awayAbbr: String
    let homeAbbr: String
    let gameTime: String?
    let gameDate: String?
    var team: String? = nil  // team abbr from Txn (e.g. "BAL"); preferred over side-logic
    var unit: Double? = nil  // when set, CardBet shows unit count after price
}

extension SelectedBet {
    init(placedBet: PlacedBet) {
        self.init(
            betHash:  placedBet.betHash,
            type:     placedBet.type,
            side:     placedBet.side,
            price:    placedBet.price,
            points:   placedBet.points,
            awayAbbr: placedBet.awayAbbr,
            homeAbbr: placedBet.homeAbbr,
            gameTime: placedBet.gameTime,
            gameDate: placedBet.gameDate
        )
    }
}
