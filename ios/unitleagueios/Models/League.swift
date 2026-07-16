import Foundation

struct League: Codable, Identifiable {
    let id: Int
    let abbr: String
    let name: String
    let sport: String
    let weather: String
    let yrOrig: Int
    let yrData: Int?

    enum CodingKeys: String, CodingKey {
        case id = "league_id"
        case abbr, name, sport, weather
        case yrOrig = "yr_orig"
        case yrData = "yr_data"
    }

    var sportIcon: String { League.sportIcon(for: id) }

    static func sportIcon(for id: Int) -> String {
        switch id {
        case 1: return "basketball"
        case 2: return "american.football"
        case 3: return "hockey.puck"
        case 4: return "baseball"
        case 5: return "american.football.fill"
        case 6: return "basketball.fill"
        default: return "sportscourt"
        }
    }
}
