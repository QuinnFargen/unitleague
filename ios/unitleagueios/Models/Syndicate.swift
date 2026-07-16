import Foundation

struct Syndicate: Codable, Identifiable {
    var id: Int { syndicateId }
    let syndicateId: Int
    let name: String
    let description: String?
    let isPublic: Bool
    let maxRunner: Int?
    let createdByBettorId: Int
    let code: String?
    let symbol: String?
    let color: String?
    let startUnits: Int?
    let isStarted: Bool

    enum CodingKeys: String, CodingKey {
        case syndicateId = "syndicate_id"
        case name
        case description
        case isPublic = "is_public"
        case fantasy
        case maxRunner = "max_runner"
        case createdByBettorId = "created_by_bettor_id"
        case code
        case symbol
        case color
        case startUnits = "start_units"
        case isStarted = "is_started"
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(syndicateId, forKey: .syndicateId)
        try c.encode(name, forKey: .name)
        try c.encodeIfPresent(description, forKey: .description)
        try c.encode(isPublic, forKey: .isPublic)
        try c.encodeIfPresent(maxRunner, forKey: .maxRunner)
        try c.encode(createdByBettorId, forKey: .createdByBettorId)
        try c.encodeIfPresent(startUnits, forKey: .startUnits)
        try c.encode(isStarted, forKey: .isStarted)
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        syndicateId = try c.decode(Int.self, forKey: .syndicateId)
        name = try c.decode(String.self, forKey: .name)
        description = try c.decodeIfPresent(String.self, forKey: .description)
        maxRunner = try c.decodeIfPresent(Int.self, forKey: .maxRunner)
        createdByBettorId = try c.decode(Int.self, forKey: .createdByBettorId)
        // mart endpoint sends "fantasy"; odd endpoint sends "is_public"
        if let fantasy = try c.decodeIfPresent(Bool.self, forKey: .fantasy) {
            isPublic = fantasy
        } else {
            isPublic = try c.decodeIfPresent(Bool.self, forKey: .isPublic) ?? false
        }
        code = try c.decodeIfPresent(String.self, forKey: .code)
        symbol = try c.decodeIfPresent(String.self, forKey: .symbol)
        color = try c.decodeIfPresent(String.self, forKey: .color)
        startUnits = try c.decodeIfPresent(Int.self, forKey: .startUnits)
        isStarted = try c.decodeIfPresent(Bool.self, forKey: .isStarted) ?? false
    }
}
