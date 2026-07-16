import Foundation

struct Bettor: Codable {
    let bettorId: Int
    let appleSub: String
    let appleEmail: String?
    let appleName: String?
    let profileName: String?
    let symbol: String?
    let color: String?

    enum CodingKeys: String, CodingKey {
        case bettorId = "bettor_id"
        case appleSub = "apple_sub"
        case appleEmail = "apple_email"
        case appleName = "apple_name"
        case profileName = "profile_name"
        case symbol
        case color
    }
}
