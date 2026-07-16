import Foundation

class RunnerService {
    func fetchRunner(bettorId: Int? = nil, syndicateId: Int? = nil) async throws -> [Runner] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/runner")!
        var queryItems: [URLQueryItem] = []
        if let bettorId    { queryItems.append(URLQueryItem(name: "bettor_id",    value: "\(bettorId)")) }
        if let syndicateId { queryItems.append(URLQueryItem(name: "syndicate_id", value: "\(syndicateId)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Runner].self, from: data)
    }
}
