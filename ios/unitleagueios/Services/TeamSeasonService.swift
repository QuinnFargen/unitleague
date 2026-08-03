import Foundation

class TeamSeasonService {
    func fetchTeamSeasons(leagueId: Int, yr: Int? = nil, conf: String? = nil,
                           color: String? = nil, region: String? = nil,
                           category: String? = nil) async throws -> [TeamSeason] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/team_season")!
        var queryItems = [URLQueryItem(name: "league_id", value: "\(leagueId)")]
        if let yr       { queryItems.append(URLQueryItem(name: "yr",       value: "\(yr)")) }
        if let conf     { queryItems.append(URLQueryItem(name: "conf",     value: conf)) }
        if let color    { queryItems.append(URLQueryItem(name: "color",    value: color)) }
        if let region   { queryItems.append(URLQueryItem(name: "region",   value: region)) }
        if let category { queryItems.append(URLQueryItem(name: "category", value: category)) }
        components.queryItems = queryItems
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([TeamSeason].self, from: data)
    }
}
