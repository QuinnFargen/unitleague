import Foundation

struct SyndicateConfig: Codable, Equatable {
    var leagueIds: [Int]?
    var blockedEnhancementTypes: [String]?
    var blockedEnhancementIds: [Int]?
    var maxClvLevel: Int?
    var maxTeamLevel: Int?
    var minUnitsWagered: Int?
    var minWagers: Int?

    enum CodingKeys: String, CodingKey {
        case leagueIds = "league_ids"
        case blockedEnhancementTypes = "blocked_enhancement_types"
        case blockedEnhancementIds = "blocked_enhancement_ids"
        case maxClvLevel = "max_clv_level"
        case maxTeamLevel = "max_team_level"
        case minUnitsWagered = "min_units_wagered"
        case minWagers = "min_wagers"
    }

    init(
        leagueIds: [Int]? = nil,
        blockedEnhancementTypes: [String]? = nil,
        blockedEnhancementIds: [Int]? = nil,
        maxClvLevel: Int? = nil,
        maxTeamLevel: Int? = nil,
        minUnitsWagered: Int? = nil,
        minWagers: Int? = nil
    ) {
        self.leagueIds = leagueIds
        self.blockedEnhancementTypes = blockedEnhancementTypes
        self.blockedEnhancementIds = blockedEnhancementIds
        self.maxClvLevel = maxClvLevel
        self.maxTeamLevel = maxTeamLevel
        self.minUnitsWagered = minUnitsWagered
        self.minWagers = minWagers
    }
}
