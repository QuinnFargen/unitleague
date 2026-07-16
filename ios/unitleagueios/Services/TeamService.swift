import Foundation

class TeamService {
    func fetchTeams(leagueId: Int, conf: String? = nil, color: String? = nil, region: String? = nil, category: String? = nil) async throws -> [Team] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/team")!
        var queryItems = [URLQueryItem(name: "league_id", value: "\(leagueId)")]
        if let conf     { queryItems.append(URLQueryItem(name: "conf",     value: conf)) }
        if let color    { queryItems.append(URLQueryItem(name: "color",    value: color)) }
        if let region   { queryItems.append(URLQueryItem(name: "region",   value: region)) }
        if let category { queryItems.append(URLQueryItem(name: "category", value: category)) }
        components.queryItems = queryItems
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Team].self, from: data)
    }
}
