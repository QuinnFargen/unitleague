import Foundation

struct Odds: Codable, Identifiable {
    var id: Int { gameId }
    let gameId: Int
    let leagueId: Int
    let gameConcat: String
    let gameDt: String
    let gameTime: String?
    let homeAbbr: String
    let awayAbbr: String
    let homeTeamId: Int
    let awayTeamId: Int
    let winner: String?
    let margin: Double?
    let total: Double?
    let hasActiveBets: Bool
    // Moneyline
    let mlHomeBetHash: String?
    let mlHomeBookmaker: String?
    let mlHomePrice: Double?
    let mlHomeWon: Bool?
    let mlHomeBetConcat: String?
    let mlAwayBetHash: String?
    let mlAwayBookmaker: String?
    let mlAwayPrice: Double?
    let mlAwayWon: Bool?
    let mlAwayBetConcat: String?
    // Spread
    let sprHomeBetHash: String?
    let sprHomeBookmaker: String?
    let sprHomePrice: Double?
    let sprHomePoints: Double?
    let sprHomeWon: Bool?
    let sprHomeBetConcat: String?
    let sprAwayBetHash: String?
    let sprAwayBookmaker: String?
    let sprAwayPrice: Double?
    let sprAwayPoints: Double?
    let sprAwayWon: Bool?
    let sprAwayBetConcat: String?
    // Over/Under
    let overBetHash: String?
    let overBookmaker: String?
    let overPrice: Double?
    let overPoints: Double?
    let overWon: Bool?
    let overBetConcat: String?
    let underBetHash: String?
    let underBookmaker: String?
    let underPrice: Double?
    let underPoints: Double?
    let underWon: Bool?
    let underBetConcat: String?
    // Meta
    let lastUpdatedTs: String?

    var sportIcon: String { League.sportIcon(for: leagueId) }

    enum CodingKeys: String, CodingKey {
        case gameId = "game_id"
        case leagueId = "league_id"
        case gameConcat = "game_concat"
        case gameDt = "game_dt"
        case gameTime = "game_ts"
        case homeAbbr = "home_abbr"
        case awayAbbr = "away_abbr"
        case homeTeamId = "home_team_id"
        case awayTeamId = "away_team_id"
        case winner
        case margin
        case total
        case hasActiveBets = "has_active_bets"
        case mlHomeBetHash = "ml_home_bet_hash"
        case mlHomeBookmaker = "ml_home_bookmaker"
        case mlHomePrice = "ml_home_price"
        case mlHomeWon = "ml_home_won"
        case mlHomeBetConcat = "ml_home_bet_concat"
        case mlAwayBetHash = "ml_away_bet_hash"
        case mlAwayBookmaker = "ml_away_bookmaker"
        case mlAwayPrice = "ml_away_price"
        case mlAwayWon = "ml_away_won"
        case mlAwayBetConcat = "ml_away_bet_concat"
        case sprHomeBetHash = "spr_home_bet_hash"
        case sprHomeBookmaker = "spr_home_bookmaker"
        case sprHomePrice = "spr_home_price"
        case sprHomePoints = "spr_home_points"
        case sprHomeWon = "spr_home_won"
        case sprHomeBetConcat = "spr_home_bet_concat"
        case sprAwayBetHash = "spr_away_bet_hash"
        case sprAwayBookmaker = "spr_away_bookmaker"
        case sprAwayPrice = "spr_away_price"
        case sprAwayPoints = "spr_away_points"
        case sprAwayWon = "spr_away_won"
        case sprAwayBetConcat = "spr_away_bet_concat"
        case overBetHash = "over_bet_hash"
        case overBookmaker = "over_bookmaker"
        case overPrice = "over_price"
        case overPoints = "over_points"
        case overWon = "over_won"
        case overBetConcat = "over_bet_concat"
        case underBetHash = "under_bet_hash"
        case underBookmaker = "under_bookmaker"
        case underPrice = "under_price"
        case underPoints = "under_points"
        case underWon = "under_won"
        case underBetConcat = "under_bet_concat"
        case lastUpdatedTs = "last_updated_ts"
    }
}
