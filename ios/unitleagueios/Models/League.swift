import Foundation

struct League: Codable, Identifiable {
    let id: Int
    let abbr: String
    let name: String
    let sport: String
    let weather: String
    let yrOrig: Int
    let yrData: Int?
    let status: String
    let seasonStartDt: String?
    let lastSchedDt: String?

    enum CodingKeys: String, CodingKey {
        case id = "league_id"
        case abbr, name, sport, weather, status
        case yrOrig       = "yr_orig"
        case yrData       = "yr_data"
        case seasonStartDt = "season_start_dt"
        case lastSchedDt   = "lastscheddate"
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
