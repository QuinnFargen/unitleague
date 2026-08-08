import Foundation

class LeagueGameDateService {
    func fetchGameDates(leagueId: Int) async throws -> [LeagueGameDate] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/league_game_dates")!
        components.queryItems = [URLQueryItem(name: "league_id", value: "\(leagueId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([LeagueGameDate].self, from: data)
    }
}
