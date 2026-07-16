import Foundation

struct TxnService {

    func submitBet(bettorId: Int, syndicateId: Int, betHash: String, unit: Double, price: Double, gameDt: String?) async throws {
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
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }

    func submitParlay(bettorId: Int, syndicateId: Int, unit: Double,
                      legs: [(betHash: String, price: Double)], gameDt: String?) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/parlay") else { throw URLError(.badURL) }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let legsArray = legs.map { ["bet_hash": $0.betHash, "price": $0.price] as [String: Any] }
        var body: [String: Any] = [
            "bettor_id": bettorId,
            "syndicate_id": syndicateId,
            "unit": unit,
            "legs": legsArray
        ]
        if let gameDt { body["game_dt"] = gameDt }
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

    func fetchCompletedBets(bettorId: Int) async throws -> [Txn] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/txn")!
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
