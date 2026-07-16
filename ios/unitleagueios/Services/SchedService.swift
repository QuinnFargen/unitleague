import Foundation

class SchedService {
    func fetchSchedule(teamId: Int? = nil, leagueId: Int? = nil, yr: Int? = nil) async throws -> [Sched] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/sched")!
        var queryItems: [URLQueryItem] = []
        if let teamId   { queryItems.append(URLQueryItem(name: "team_id",   value: "\(teamId)")) }
        if let leagueId { queryItems.append(URLQueryItem(name: "league_id", value: "\(leagueId)")) }
        if let yr       { queryItems.append(URLQueryItem(name: "yr",        value: "\(yr)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Sched].self, from: data)
    }
}
