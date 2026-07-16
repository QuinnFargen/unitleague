import Foundation

class EnhancementService {
    func fetchOptions(bettorId: Int, syndicateId: Int) async throws -> [EnhanceOption] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/enhance_options")!
        components.queryItems = [
            URLQueryItem(name: "bettor_id",    value: "\(bettorId)"),
            URLQueryItem(name: "syndicate_id", value: "\(syndicateId)"),
        ]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([EnhanceOption].self, from: data)
    }

    func fetchEnhanced(bettorId: Int? = nil, syndicateId: Int? = nil) async throws -> [Enhanced] {
        var components = URLComponents(string: "\(APIClient.baseURL)/odd/enhanced")!
        var queryItems: [URLQueryItem] = []
        if let bettorId    { queryItems.append(URLQueryItem(name: "bettor_id",    value: "\(bettorId)")) }
        if let syndicateId { queryItems.append(URLQueryItem(name: "syndicate_id", value: "\(syndicateId)")) }
        if !queryItems.isEmpty { components.queryItems = queryItems }
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Enhanced].self, from: data)
    }

    func chooseEnhancement(bettorId: Int, syndicateId: Int, enhancementId: Int, teamId: Int, level: Int, optionHash: String) async throws -> Enhanced {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/enhanced") else { throw URLError(.badURL) }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body: [String: Any] = [
            "bettor_id":      bettorId,
            "syndicate_id":   syndicateId,
            "enhancement_id": enhancementId,
            "team_id":        teamId,
            "level":          level,
            "option_hash":    optionHash,
        ]
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, _) = try await URLSession.shared.data(for: request)
        return try JSONDecoder().decode(Enhanced.self, from: data)
    }
}
