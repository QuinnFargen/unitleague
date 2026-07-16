import Foundation

class OddsService {
    func fetchOddBest(gameId: Int? = nil, gameDt: String? = nil, leagueId: Int? = nil) async throws -> [Odds] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/game_oddbest")!
        var queryItems: [URLQueryItem] = []
        if let gameId    { queryItems.append(URLQueryItem(name: "game_id",   value: "\(gameId)")) }
        if let gameDt    { queryItems.append(URLQueryItem(name: "game_dt",   value: gameDt)) }
        if let leagueId  { queryItems.append(URLQueryItem(name: "league_id", value: "\(leagueId)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Odds].self, from: data)
    }
}
