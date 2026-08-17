import Foundation

struct Bettor: Codable {
    let bettorId: Int
    let appleSub: String
    let appleEmail: String?
    let appleName: String?
    let profileName: String?
    let symbol: String?
    let color: String?
    let favoriteTeamId: Int?

    enum CodingKeys: String, CodingKey {
        case bettorId = "bettor_id"
        case appleSub = "apple_sub"
        case appleEmail = "apple_email"
        case appleName = "apple_name"
        case profileName = "profile_name"
        case symbol
        case color
        case favoriteTeamId = "favorite_team_id"
    }
}

struct BettorStats: Codable, Identifiable {
    var id: Int { bettorId }
    let bettorId: Int
    let profileName: String?
    let symbol: String?
    let color: String?
    let favoriteTeamId: Int?
    let favoriteTeamAbbr: String?
    let favoriteLeagueId: Int?
    let careerBalance: Double

    enum CodingKeys: String, CodingKey {
        case bettorId = "bettor_id"
        case profileName = "profile_name"
        case symbol
        case color
        case favoriteTeamId = "favorite_team_id"
        case favoriteTeamAbbr = "favorite_team_abbr"
        case favoriteLeagueId = "favorite_league_id"
        case careerBalance = "career_balance"
    }
}

struct BettorLeagueBalance: Codable, Identifiable {
    var id: Int { leagueId }
    let bettorId: Int
    let leagueId: Int
    let balance: Double

    enum CodingKeys: String, CodingKey {
        case bettorId = "bettor_id"
        case leagueId = "league_id"
        case balance
    }
}
