import Foundation

struct TxnService {

    func submitBet(bettorId: Int, syndicateId: Int, betHash: String, unit: Double, price: Double, gameDt: String?,
                   priceEnhanced: Double? = nil, unitEnhanced: Double? = nil) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/txn") else { throw URLError(.badURL) }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        var body: [String: Any] = [
            "bettor_id": bettorId,
            "syndicate_id": syndicateId,
            "bet_hash": betHash,
            "unit": unit,
            "price": price
        ]
        if let gameDt { body["game_dt"] = gameDt }
        if let priceEnhanced { body["price_enhanced"] = priceEnhanced }
        if let unitEnhanced { body["unit_enhanced"] = unitEnhanced }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }

    func submitParlay(bettorId: Int, syndicateId: Int, unit: Double,
                      legs: [(betHash: String, price: Double, priceEnhanced: Double?)], gameDt: String?,
                      unitEnhanced: Double? = nil) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/parlay") else { throw URLError(.badURL) }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let legsArray = legs.map { leg -> [String: Any] in
            var legDict: [String: Any] = ["bet_hash": leg.betHash, "price": leg.price]
            if let priceEnhanced = leg.priceEnhanced { legDict["price_enhanced"] = priceEnhanced }
            return legDict
        }
        var body: [String: Any] = [
            "bettor_id": bettorId,
            "syndicate_id": syndicateId,
            "unit": unit,
            "legs": legsArray
        ]
        if let gameDt { body["game_dt"] = gameDt }
        if let unitEnhanced { body["unit_enhanced"] = unitEnhanced }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }

    func fetchActiveBets(bettorId: Int) async throws -> [Txn] {
        var components = URLComponents(string: "\(APIClient.baseURL)/odd/txn")!
        components.queryItems = [URLQueryItem(name: "bettor_id", value: "\(bettorId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Txn].self, from: data)
    }

    func fetchActiveBets(syndicateId: Int) async throws -> [Txn] {
        var components = URLComponents(string: "\(APIClient.baseURL)/odd/txn")!
        components.queryItems = [URLQueryItem(name: "syndicate_id", value: "\(syndicateId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Txn].self, from: data)
    }

    func fetchCompletedBets(bettorId: Int) async throws -> [Txn] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/txn")!
        components.queryItems = [URLQueryItem(name: "bettor_id", value: "\(bettorId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Txn].self, from: data)
    }

    func fetchTxnBets(bettorId: Int) async throws -> [Txn] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/txn/bets")!
        components.queryItems = [URLQueryItem(name: "bettor_id", value: "\(bettorId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Txn].self, from: data)
    }

    func cancelTxn(txnId: Int) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/txn/\(txnId)/cancel") else { throw URLError(.badURL) }
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        let (_, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }
}
