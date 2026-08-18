import Foundation

struct Week: Codable, Identifiable {
    var id: Int { weekId }
    let weekId: Int
    let seasonId: Int
    let leagueId: Int
    let weekStartDt: String
    let weekEndDt: String
    let weekNum: Int
    let weekConcat: String?
    let weekName: String
    let isPre: Bool
    let isPost: Bool

    enum CodingKeys: String, CodingKey {
        case weekId = "week_id"
        case seasonId = "season_id"
        case leagueId = "league_id"
        case weekStartDt = "week_start_dt"
        case weekEndDt = "week_end_dt"
        case weekNum = "week_num"
        case weekConcat = "week_concat"
        case weekName = "week_name"
        case isPre = "is_pre"
        case isPost = "is_post"
    }
}
