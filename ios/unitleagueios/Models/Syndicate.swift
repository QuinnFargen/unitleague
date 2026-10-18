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
    let config: SyndicateConfig?
    let syndicateType: String?
    let leagueId: Int?
    let yr: Int?
    /// Position in the bettor's Syndicate tab (from their runner row); nil means hidden.
    var sortOrder: Int?

    var isHidden: Bool { sortOrder == nil }

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
        case config
        case syndicateType = "syndicate_type"
        case leagueId = "league_id"
        case yr
        case sortOrder = "sort_order"
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(syndicateId, forKey: .syndicateId)
        try c.encode(name, forKey: .name)
        try c.encodeIfPresent(description, forKey: .description)
        try c.encode(isPublic, forKey: .isPublic)
        try c.encodeIfPresent(maxRunner, forKey: .maxRunner)
        try c.encode(createdByBettorId, forKey: .createdByBettorId)
        try c.encodeIfPresent(code, forKey: .code)
        try c.encodeIfPresent(symbol, forKey: .symbol)
        try c.encodeIfPresent(color, forKey: .color)
        try c.encodeIfPresent(startUnits, forKey: .startUnits)
        try c.encode(isStarted, forKey: .isStarted)
        try c.encodeIfPresent(config, forKey: .config)
        try c.encodeIfPresent(syndicateType, forKey: .syndicateType)
        try c.encodeIfPresent(leagueId, forKey: .leagueId)
        try c.encodeIfPresent(yr, forKey: .yr)
        try c.encodeIfPresent(sortOrder, forKey: .sortOrder)
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
        config = try c.decodeIfPresent(SyndicateConfig.self, forKey: .config)
        syndicateType = try c.decodeIfPresent(String.self, forKey: .syndicateType)
        leagueId = try c.decodeIfPresent(Int.self, forKey: .leagueId)
        yr = try c.decodeIfPresent(Int.self, forKey: .yr)
        sortOrder = try c.decodeIfPresent(Int.self, forKey: .sortOrder)
    }
}

// MARK: - Memberwise init (custom Decodable above suppresses the synthesized one)

extension Syndicate {
    init(
        syndicateId: Int,
        name: String,
        description: String? = nil,
        isPublic: Bool = true,
        maxRunner: Int? = nil,
        createdByBettorId: Int = 1,
        code: String? = nil,
        symbol: String? = nil,
        color: String? = nil,
        startUnits: Int? = nil,
        isStarted: Bool = false,
        config: SyndicateConfig? = nil,
        syndicateType: String? = nil,
        leagueId: Int? = nil,
        yr: Int? = nil,
        sortOrder: Int? = 1
    ) {
        self.syndicateId = syndicateId
        self.name = name
        self.description = description
        self.isPublic = isPublic
        self.maxRunner = maxRunner
        self.createdByBettorId = createdByBettorId
        self.code = code
        self.symbol = symbol
        self.color = color
        self.startUnits = startUnits
        self.isStarted = isStarted
        self.config = config
        self.syndicateType = syndicateType
        self.leagueId = leagueId
        self.yr = yr
        self.sortOrder = sortOrder
    }
}
