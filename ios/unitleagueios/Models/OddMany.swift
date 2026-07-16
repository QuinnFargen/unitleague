import Foundation

struct OddMany: Codable, Identifiable {
    var id: String { betHash }
    let betHash: String
    let bookmaker: String
    let gameId: Int
    let leagueId: Int
    let gameDt: String
    let gameTime: String?
    let homeAbbr: String
    let awayAbbr: String
    let teamId: Int?
    let betType: String
    let betConcat: String
    let price: Double
    let points: Double?
    let startTs: String
    let teamAbbr: String?

    enum CodingKeys: String, CodingKey {
        case betHash = "bet_hash"
        case bookmaker
        case gameId = "game_id"
        case leagueId = "league_id"
        case gameDt = "game_dt"
        case gameTime = "game_ts"
        case homeAbbr = "home_abbr"
        case awayAbbr = "away_abbr"
        case teamId = "team_id"
        case betType = "bet_type"
        case betConcat = "bet_concat"
        case price
        case points
        case startTs = "start_ts"
        case teamAbbr = "team_abbr"
    }
}
