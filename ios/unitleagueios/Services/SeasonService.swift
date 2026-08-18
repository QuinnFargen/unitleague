import Foundation

class SeasonService {
    func fetchSeasons(leagueId: Int? = nil, active: Bool? = nil) async throws -> [Season] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/season")!
        var queryItems: [URLQueryItem] = []
        if let leagueId { queryItems.append(URLQueryItem(name: "league_id", value: "\(leagueId)")) }
        if let active    { queryItems.append(URLQueryItem(name: "active",    value: "\(active)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Season].self, from: data)
    }
}
