import Foundation

class TeamOddsRecentService {
    func fetchTeamOddsRecent(leagueId: Int) async throws -> [TeamOddsRecent] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/team_odds_recent")!
        components.queryItems = [URLQueryItem(name: "league_id", value: "\(leagueId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([TeamOddsRecent].self, from: data)
    }
}
