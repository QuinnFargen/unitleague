import Foundation

struct TeamOddsRecent: Codable, Identifiable {
    var id: String { "\(teamId)_\(seasonId)" }
    let teamId: Int
    let leagueId: Int
    let abbr: String
    let conf: String?
    let color: String?
    let region: String?
    let category: String?
    let seasonId: Int
    let seasonConcat: String?
    let yr: Int
    let atsWins: Int
    let atsLosses: Int
    let atsPushes: Int
    let atsLast10Str: String?
    let atsCoverPct: Double?
    let overCount: Int
    let underCount: Int
    let ouPushes: Int
    let ouLast10Str: String?
    let overPct: Double?

    enum CodingKeys: String, CodingKey {
        case teamId       = "team_id"
        case leagueId     = "league_id"
        case abbr
        case conf, color, region, category
        case seasonId     = "season_id"
        case seasonConcat = "season_concat"
        case yr
        case atsWins      = "ats_wins"
        case atsLosses    = "ats_losses"
        case atsPushes    = "ats_pushes"
        case atsLast10Str = "ats_last10_str"
        case atsCoverPct  = "ats_cover_pct"
        case overCount    = "over_count"
        case underCount   = "under_count"
        case ouPushes     = "ou_pushes"
        case ouLast10Str  = "ou_last10_str"
        case overPct      = "over_pct"
    }
}
