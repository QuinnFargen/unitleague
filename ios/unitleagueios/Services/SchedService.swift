import Foundation

class SchedService {
    func fetchSchedule(teamId: Int? = nil, leagueId: Int? = nil, yr: Int? = nil,
                        oppConf: String? = nil, oppColor: String? = nil,
                        oppRegion: String? = nil, oppMascot: String? = nil,
                        limit: Int? = nil) async throws -> [Sched] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/sched")!
        var queryItems: [URLQueryItem] = []
        if let teamId    { queryItems.append(URLQueryItem(name: "team_id",    value: "\(teamId)")) }
        if let leagueId  { queryItems.append(URLQueryItem(name: "league_id",  value: "\(leagueId)")) }
        if let yr        { queryItems.append(URLQueryItem(name: "yr",         value: "\(yr)")) }
        if let oppConf   { queryItems.append(URLQueryItem(name: "opp_conf",   value: oppConf)) }
        if let oppColor  { queryItems.append(URLQueryItem(name: "opp_color",  value: oppColor)) }
        if let oppRegion { queryItems.append(URLQueryItem(name: "opp_region", value: oppRegion)) }
        if let oppMascot { queryItems.append(URLQueryItem(name: "opp_mascot", value: oppMascot)) }
        if let limit     { queryItems.append(URLQueryItem(name: "limit",      value: "\(limit)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Sched].self, from: data)
    }
}
