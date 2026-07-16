import Foundation

struct Runner: Codable, Identifiable {
    var id: Int { runnerId }
    let runnerId: Int
    let bettorId: Int
    let syndicateId: Int
    let role: String
    let active: Bool?
    let balance: Int?
    let profileName: String?
    let symbol: String?
    let color: String?

    enum CodingKeys: String, CodingKey {
        case runnerId = "runner_id"
        case bettorId = "bettor_id"
        case syndicateId = "syndicate_id"
        case role
        case active
        case balance
        case profileName = "profile_name"
        case symbol
        case color
    }
}
