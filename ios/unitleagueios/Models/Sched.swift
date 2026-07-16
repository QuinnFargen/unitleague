import Foundation

struct Sched: Codable, Identifiable {
    let id: String
    let teamAbbr: String
    let oppAbbr: String?
    let gameDate: String?
    let gameNum: Int
    let yr: Int
    let schedConcat: String?
    let teamScore: Int?
    let oppScore: Int?
    let home: Bool
    let won: Bool?
    let gameId: Int?
    let leagueId: Int
    let teamId: Int
    let oppTeamId: Int?

    enum CodingKeys: String, CodingKey {
        case id          = "sched_id"
        case teamAbbr    = "team_abbr"
        case oppAbbr     = "opp_abbr"
        case gameDate    = "game_dt"
        case gameNum     = "game_num"
        case yr
        case schedConcat = "sched_concat"
        case teamScore   = "team"
        case oppScore    = "opp"
        case home, won
        case gameId      = "game_id"
        case leagueId    = "league_id"
        case teamId      = "team_id"
        case oppTeamId   = "opp_team_id"
    }
}
