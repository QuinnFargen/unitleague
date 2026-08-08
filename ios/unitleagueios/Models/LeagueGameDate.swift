import Foundation

struct LeagueGameDate: Codable {
    let leagueId: Int
    let gameDt: String
    let weekNum: Int?
    let yr: Int?

    enum CodingKeys: String, CodingKey {
        case leagueId = "league_id"
        case gameDt   = "game_dt"
        case weekNum  = "week_num"
        case yr
    }
}
