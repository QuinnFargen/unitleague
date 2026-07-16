import Foundation

struct Game: Codable, Identifiable {
    let id: Int
    let home: String
    let away: String
    let gameDate: String
    let gameTime: String?
    let homeScore: Int?
    let awayScore: Int?
    let winner: String?
    let leagueId: Int
    let homeTeamId: Int
    let awayTeamId: Int
    let wonTeamId: Int?

    enum CodingKeys: String, CodingKey {
        case id         = "game_id"
        case home, away
        case gameDate   = "game_dt"
        case gameTime   = "game_ts"
        case homeScore  = "h"
        case awayScore  = "a"
        case winner
        case leagueId   = "league_id"
        case homeTeamId = "home_team_id"
        case awayTeamId = "away_team_id"
        case wonTeamId  = "won_team_id"
    }

    var sportIcon: String { League.sportIcon(for: leagueId) }
}
