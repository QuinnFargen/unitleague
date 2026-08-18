import Foundation

struct Season: Codable, Identifiable {
    var id: Int { seasonId }
    let seasonId: Int
    let leagueId: Int
    let seasonConcat: String?
    let yr: Int
    let yrVar: String?
    let preDt: String?
    let regStartDt: String?
    let regEndDt: String?
    let postStartDt: String?
    let champSeriesStartDt: String?
    let champDt: String?
    let active: Bool
    let champTeamId: Int?
    let champAbbr: String?

    enum CodingKeys: String, CodingKey {
        case seasonId = "season_id"
        case leagueId = "league_id"
        case seasonConcat = "season_concat"
        case yr
        case yrVar = "yr_var"
        case preDt = "pre_dt"
        case regStartDt = "reg_start_dt"
        case regEndDt = "reg_end_dt"
        case postStartDt = "post_start_dt"
        case champSeriesStartDt = "champ_series_start_dt"
        case champDt = "champ_dt"
        case active
        case champTeamId = "champ_team_id"
        case champAbbr = "champ_abbr"
    }
}
