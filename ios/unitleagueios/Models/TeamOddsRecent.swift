import Foundation

struct TeamOddsRecent: Codable, Identifiable {
    var id: Int { teamId }
    let teamId: Int
    let leagueId: Int
    let abbr: String
    let conf: String?
    let color: String?
    let region: String?
    let category: String?
    let atsWins: Int
    let atsLosses: Int
    let atsLast10Str: String?
    let overCount: Int
    let underCount: Int
    let ouLast10Str: String?

    enum CodingKeys: String, CodingKey {
        case teamId       = "team_id"
        case leagueId     = "league_id"
        case abbr
        case conf, color, region, category
        case atsWins      = "ats_wins"
        case atsLosses    = "ats_losses"
        case atsLast10Str = "ats_last10_str"
        case overCount    = "over_count"
        case underCount   = "under_count"
        case ouLast10Str  = "ou_last10_str"
    }
}
