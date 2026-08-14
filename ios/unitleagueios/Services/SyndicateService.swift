import Foundation

class SyndicateService {
    func fetchSyndicate(syndicateId: Int? = nil, bettorId: Int? = nil) async throws -> [Syndicate] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/syndicate")!
        var queryItems: [URLQueryItem] = []
        if let syndicateId { queryItems.append(URLQueryItem(name: "syndicate_id", value: "\(syndicateId)")) }
        if let bettorId    { queryItems.append(URLQueryItem(name: "bettor_id",    value: "\(bettorId)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Syndicate].self, from: data)
    }

    func createSyndicate(bettorId: Int, name: String, description: String? = nil, isPublic: Bool = false, password: String? = nil, symbol: String? = nil, color: String? = nil, syndicateType: String? = nil, leagueIds: [Int]? = nil) async throws -> Syndicate {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/syndicate") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = ["bettor_id": bettorId, "name": name, "is_public": isPublic]
        if let desc = description { body["description"] = desc }
        if let pw = password { body["password"] = pw }
        if let sym = symbol { body["symbol"] = sym }
        if let col = color { body["color"] = col }
        if let st = syndicateType { body["syndicate_type"] = st }
        if let ids = leagueIds { body["league_ids"] = ids }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, _) = try await URLSession.shared.data(for: request)

        struct CreateSyndicateResponse: Codable {
            let syndicate: Syndicate
            let runner: Runner
        }
        return try JSONDecoder().decode(CreateSyndicateResponse.self, from: data).syndicate
    }

    func startSyndicate(syndicateId: Int, bettorId: Int) async throws -> Syndicate {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/syndicate/\(syndicateId)/start") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: ["bettor_id": bettorId])

        let (data, _) = try await URLSession.shared.data(for: request)

        struct StartResponse: Codable {
            let syndicate: Syndicate
        }
        return try JSONDecoder().decode(StartResponse.self, from: data).syndicate
    }

    func updateSyndicate(syndicateId: Int, name: String? = nil, description: String? = nil, symbol: String? = nil, color: String? = nil, config: SyndicateConfig? = nil, maxRunner: Int? = nil, startUnits: Int? = nil) async throws -> Syndicate {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/syndicate/\(syndicateId)") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = [:]
        if let name = name { body["name"] = name }
        if let description = description { body["description"] = description }
        if let sym = symbol { body["symbol"] = sym }
        if let col = color  { body["color"] = col }
        if let max = maxRunner { body["max_runner"] = max }
        if let su = startUnits { body["start_units"] = su }
        if let config = config {
            let configData = try JSONEncoder().encode(config)
            body["config"] = try JSONSerialization.jsonObject(with: configData)
        }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, _) = try await URLSession.shared.data(for: request)
        return try JSONDecoder().decode(Syndicate.self, from: data)
    }

    func joinSyndicate(bettorId: Int, code: String, password: String? = nil) async throws -> Runner {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/syndicate/join/\(code)") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = ["bettor_id": bettorId]
        if let pw = password { body["password"] = pw }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await URLSession.shared.data(for: request)
        if let http = response as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
            let detail = (try? JSONDecoder().decode([String: String].self, from: data))?["detail"]
            throw SyndicateError.message(detail ?? "Unable to join syndicate.")
        }
        return try JSONDecoder().decode(Runner.self, from: data)
    }

    func fetchPublicSyndicates(bettorId: Int, leagueId: Int? = nil) async throws -> [Syndicate] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/syndicate/public")!
        var queryItems: [URLQueryItem] = [URLQueryItem(name: "bettor_id", value: "\(bettorId)")]
        if let leagueId { queryItems.append(URLQueryItem(name: "league_id", value: "\(leagueId)")) }
        components.queryItems = queryItems
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Syndicate].self, from: data)
    }
}

enum SyndicateError: LocalizedError {
    case message(String)

    var errorDescription: String? {
        switch self {
        case .message(let text): return text
        }
    }
}
