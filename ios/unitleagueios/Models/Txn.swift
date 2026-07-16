import Foundation

struct Txn: Codable, Identifiable {
    let id: UUID = UUID()
    let txnId: Int
    let bettorId: Int
    let syndicateId: Int
    let txnType: String
    let betHash: String?
    let parlayId: Int?
    let unit: Double
    let price: Double
    let won: Bool?
    let canceled: Bool?
    let cancelTs: String?
    let insertDt: String?
    let betType: String?
    let points: Double?
    let team: String?
    let home: String?
    let away: String?
    let gameTime: String?
    let gameDate: String?
    let gameId: Int?
    let bookmaker: String?
    let betConcat: String?
    let parlayPriceMult: Double?
    let unitEnhanced: Double?
    let priceEnhanced: Double?

    enum CodingKeys: String, CodingKey {
        case txnId           = "txn_id"
        case bettorId        = "bettor_id"
        case syndicateId     = "syndicate_id"
        case txnType         = "txn_type"
        case betHash         = "bet_hash"
        case parlayId        = "parlay_id"
        case unit, price, won, canceled, points, team, home, away, bookmaker
        case cancelTs        = "cancel_ts"
        case insertDt        = "insert_dt"
        case betType         = "bet_type"
        case gameTime        = "game_ts"
        case gameDate        = "game_dt"
        case gameId          = "game_id"
        case betConcat       = "bet_concat"
        case parlayPriceMult = "parlay_price_mult"
        case unitEnhanced    = "unit_enhanced"
        case priceEnhanced   = "price_enhanced"
    }
}
