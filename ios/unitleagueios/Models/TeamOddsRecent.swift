import Foundation

struct TeamOddsRecent: Codable, Identifiable {
    var id: Int { teamId }
    let teamId: Int
    let leagueId: Int
    let abbr: String
    let atsWins: Int
    let atsLosses: Int
    let atsLast10Str: String?
    let overCount: Int
    let underCount: Int

    enum CodingKeys: String, CodingKey {
        case teamId       = "team_id"
        case leagueId     = "league_id"
        case abbr
        case atsWins      = "ats_wins"
        case atsLosses    = "ats_losses"
        case atsLast10Str = "ats_last10_str"
        case overCount    = "over_count"
        case underCount   = "under_count"
    }
}
