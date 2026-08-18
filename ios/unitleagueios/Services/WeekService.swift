import Foundation

class WeekService {
    func fetchWeeks(seasonId: Int? = nil, leagueId: Int? = nil) async throws -> [Week] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/week")!
        var queryItems: [URLQueryItem] = []
        if let seasonId { queryItems.append(URLQueryItem(name: "season_id", value: "\(seasonId)")) }
        if let leagueId { queryItems.append(URLQueryItem(name: "league_id", value: "\(leagueId)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Week].self, from: data)
    }
}
