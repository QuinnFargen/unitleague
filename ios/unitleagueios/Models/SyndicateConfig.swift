import Foundation

enum SyndicateType: String, CaseIterable {
    case h2h = "H2H"
    case unit = "Unit"
    case team = "Team"
}

struct SyndicateConfig: Codable, Equatable {
    var leagueIds: [Int]?
    var syndicateType: String?
    var blockedEnhancementTypes: [String]?
    var blockedEnhancementIds: [Int]?
    var maxClvLevel: Int?
    var maxTeamLevel: Int?
    var minUnitsWagered: Int?
    var minWagers: Int?
    /// Multiplies every enhancement's base cost (1/2/3/5/8). Allowed values: 2, 3, 4. nil/1 means no multiplier.
    var costMultiplier: Int?
    /// Max number of active Edge-type enhancements a runner may hold at once. nil means no limit.
    var maxActiveEdges: Int?

    enum CodingKeys: String, CodingKey {
        case leagueIds = "league_ids"
        case syndicateType = "syndicate_type"
        case blockedEnhancementTypes = "blocked_enhancement_types"
        case blockedEnhancementIds = "blocked_enhancement_ids"
        case maxClvLevel = "max_clv_level"
        case maxTeamLevel = "max_team_level"
        case minUnitsWagered = "min_units_wagered"
        case minWagers = "min_wagers"
        case costMultiplier = "cost_multiplier"
        case maxActiveEdges = "max_active_edges"
    }

    init(
        leagueIds: [Int]? = nil,
        syndicateType: String? = nil,
        blockedEnhancementTypes: [String]? = nil,
        blockedEnhancementIds: [Int]? = nil,
        maxClvLevel: Int? = nil,
        maxTeamLevel: Int? = nil,
        minUnitsWagered: Int? = nil,
        minWagers: Int? = nil,
        costMultiplier: Int? = nil,
        maxActiveEdges: Int? = nil
    ) {
        self.leagueIds = leagueIds
        self.syndicateType = syndicateType
        self.blockedEnhancementTypes = blockedEnhancementTypes
        self.blockedEnhancementIds = blockedEnhancementIds
        self.maxClvLevel = maxClvLevel
        self.maxTeamLevel = maxTeamLevel
        self.minUnitsWagered = minUnitsWagered
        self.minWagers = minWagers
        self.costMultiplier = costMultiplier
        self.maxActiveEdges = maxActiveEdges
    }
}
