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
    let symbol: String?
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
        case symbol
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
    let symbol: String?
    let leagueId: Int?
    let level: Int

    enum CodingKeys: String, CodingKey {
        case bettorId      = "bettor_id"
        case syndicateId   = "syndicate_id"
        case runnerId      = "runner_id"
        case teamId        = "team_id"
        case enhancementType = "enhancement_type"
        case name
        case symbol
        case leagueId      = "league_id"
        case level
    }
}

extension Enhanced {
    static func clvMultiplier(level: Int?) -> Double {
        guard let level, level > 0 else { return 1.0 }
        return Double(level) * 0.1 + 1.0
    }
}

struct EnhancementDef: Codable, Identifiable {
    var id: Int { enhancementId }
    let enhancementId: Int
    let enhancementType: String
    let name: String
    let description: String?
    let symbol: String?
    let betType: String?
    let active: Bool

    enum CodingKeys: String, CodingKey {
        case enhancementId = "enhancement_id"
        case enhancementType = "enhancement_type"
        case name, description, symbol
        case betType = "bet_type"
        case active
    }
}
