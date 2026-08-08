import Foundation

struct TeamSeason: Codable, Identifiable {
    var id: String { "\(teamId)_\(seasonId)" }
    let teamId: Int
    let abbr: String
    let leagueId: Int
    let conf: String?
    let color: String?
    let region: String?
    let category: String?
    let seasonId: Int
    let seasonConcat: String?
    let yr: Int
    let gamesPlayed: Int
    let wins: Int
    let losses: Int
    let winPct: Double
    let last5Str: String?
    let last10Str: String?
    let last5Wins: Int?
    let last10Wins: Int?
    let last15Wins: Int?

    enum CodingKeys: String, CodingKey {
        case teamId       = "team_id"
        case abbr
        case leagueId     = "league_id"
        case conf, color, region, category
        case seasonId     = "season_id"
        case seasonConcat = "season_concat"
        case yr
        case gamesPlayed  = "games_played"
        case wins, losses
        case winPct       = "win_pct"
        case last5Str     = "last_5_str"
        case last10Str    = "last_10_str"
        case last5Wins    = "last_5_wins"
        case last10Wins   = "last_10_wins"
        case last15Wins   = "last_15_wins"
    }
}
