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
    let availableAttrValue: String?
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
        case availableAttrValue = "available_attr_value"
        case optionHash    = "option_hash"
    }
}

struct Enhanced: Codable, Identifiable {
    var id: String { "\(bettorId)-\(syndicateId)-\(runnerId ?? 0)-\(teamId)-\(enhancementType)-\(name)" }
    let bettorId: Int
    let syndicateId: Int
    let runnerId: Int?
    let teamId: Int
    let enhancementType: String
    let name: String
    let leagueId: Int?
    let level: Int

    enum CodingKeys: String, CodingKey {
        case bettorId      = "bettor_id"
        case syndicateId   = "syndicate_id"
        case runnerId      = "runner_id"
        case teamId        = "team_id"
        case enhancementType = "enhancement_type"
        case name
        case leagueId      = "league_id"
        case level
    }
}
