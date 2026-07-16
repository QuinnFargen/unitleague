import Foundation

struct EnhanceOption: Codable, Identifiable {
    var id: String { optionHash }
    let runnerId: Int
    let bettorId: Int
    let syndicateId: Int
    let enhancementId: Int
    let enhancementType: String
    let name: String
    let description: String
    let betType: String?
    let leagueId: Int
    let optionHash: String

    enum CodingKeys: String, CodingKey {
        case runnerId      = "runner_id"
        case bettorId      = "bettor_id"
        case syndicateId   = "syndicate_id"
        case enhancementId = "enhancement_id"
        case enhancementType = "enhancement_type"
        case name, description
        case betType       = "bet_type"
        case leagueId      = "league_id"
        case optionHash    = "option_hash"
    }
}

struct Enhanced: Codable, Identifiable {
    var id: Int { enhancedId }
    let enhancedId: Int
    let bettorId: Int
    let syndicateId: Int
    let enhancementId: Int
    let teamId: Int?
    let level: Int
    let optionHash: String
    let active: Bool

    enum CodingKeys: String, CodingKey {
        case enhancedId    = "enhanced_id"
        case bettorId      = "bettor_id"
        case syndicateId   = "syndicate_id"
        case enhancementId = "enhancement_id"
        case teamId        = "team_id"
        case level
        case optionHash    = "option_hash"
        case active
    }
}
