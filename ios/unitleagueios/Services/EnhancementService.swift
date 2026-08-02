import Foundation

enum EnhancementError: LocalizedError {
    case limitReached
    case message(String)

    var errorDescription: String? {
        switch self {
        case .limitReached: return "Edge limit reached; choose an edge to sell."
        case .message(let text): return text
        }
    }
}

class EnhancementService {
    func fetchOptions(bettorId: Int, syndicateId: Int) async throws -> [EnhanceOption] {
        var components = URLComponents(string: "\(APIClient.baseURL)/odd/enhance_options")!
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

    func fetchEnhancements(type: String? = nil, active: Bool = true) async throws -> [EnhancementDef] {
        var components = URLComponents(string: "\(APIClient.baseURL)/odd/enhancement")!
        var queryItems: [URLQueryItem] = [URLQueryItem(name: "active", value: "\(active)")]
        if let type { queryItems.append(URLQueryItem(name: "enhancement_type", value: type)) }
        components.queryItems = queryItems
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([EnhancementDef].self, from: data)
    }

    func chooseEnhancement(
        bettorId: Int, syndicateId: Int, runnerId: Int, enhancementId: Int,
        teamId: Int, level: Int, optionHash: String, sellEnhancedId: Int? = nil
    ) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/enhanced") else { throw URLError(.badURL) }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        var body: [String: Any] = [
            "bettor_id":      bettorId,
            "syndicate_id":   syndicateId,
            "runner_id":      runnerId,
            "enhancement_id": enhancementId,
            "team_id":        teamId,
            "level":          level,
            "option_hash":    optionHash,
        ]
        if let sellEnhancedId { body["sell_enhanced_id"] = sellEnhancedId }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw URLError(.badServerResponse) }
        if http.statusCode == 409 { throw EnhancementError.limitReached }
        if !(200..<300).contains(http.statusCode) {
            let detail = (try? JSONDecoder().decode([String: String].self, from: data))?["detail"]
            throw EnhancementError.message(detail ?? "Unable to choose enhancement.")
        }
    }

    func fetchActiveEdges(runnerId: Int) async throws -> [EnhancedRow] {
        var components = URLComponents(string: "\(APIClient.baseURL)/odd/enhanced/runner/\(runnerId)")!
        components.queryItems = [URLQueryItem(name: "enhancement_type", value: "edge")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([EnhancedRow].self, from: data)
    }
}
