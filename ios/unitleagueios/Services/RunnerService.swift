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

    func updateRunner(runnerId: Int, profileName: String? = nil, symbol: String? = nil, color: String? = nil) async throws -> Runner {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/runner/\(runnerId)") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = [:]
        if let name = profileName { body["profile_name"] = name }
        if let sym = symbol       { body["symbol"] = sym }
        if let col = color        { body["color"] = col }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, _) = try await URLSession.shared.data(for: request)
        return try JSONDecoder().decode(Runner.self, from: data)
    }
}
