import Foundation
import SwiftUI

struct Team: Codable, Identifiable {
    let id: Int
    let leagueId: Int
    let abbr: String
    let teamConcat: String
    let name: String
    let location: String?
    let conf: String?
    let div: String?
    let lat: Double?
    let lon: Double?
    let weather: Int
    let mascot: String?
    let color: String?
    let region: String?
    let category: String?

    enum CodingKeys: String, CodingKey {
        case id         = "team_id"
        case leagueId   = "league_id"
        case abbr
        case teamConcat = "team_concat"
        case name, location, conf, div, lat, lon, weather, mascot, color, region, category
    }

    var teamColor: Color {
        switch color ?? "" {
        case "Blue":    return Color(red: 0.10, green: 0.25, blue: 0.80)
        case "Red":     return .red
        case "Green":   return .green
        case "Yellow":  return .yellow
        case "Black":   return Color(white: 0.15)
        case "Purple":  return .purple
        case "Orange":  return .orange
        case "Brown":   return Color(red: 0.55, green: 0.27, blue: 0.07)
        case "White":   return .white
        case "Gray":    return .gray
        case "Gold":    return Color(red: 1.0, green: 0.75, blue: 0.0)
        case "Navy":    return Color(red: 0.0, green: 0.0, blue: 0.50)
        default:        return .gray
        }
    }

    var categoryIcon: String {
        switch category ?? "" {
        case "Person":    return "person"
        case "Animal":    return "pawprint"
        case "Bird":      return "bird"
        case "Cat":       return "cat"
        case "Dog":       return "dog"
        case "Color":     return "paintpalette"
        case "Imaginary": return "person.fill.questionmark"
        default:          return "questionmark"
        }
    }

    var regionIcon: String {
        switch region ?? "" {
        case "East":    return "arrowshape.right.fill"
        case "West":    return "arrowshape.left.fill"
        case "South":   return "arrowshape.down.fill"
        case "Midwest": return "arrowshape.up.fill"
        default:        return "questionmark"
        }
    }
}
