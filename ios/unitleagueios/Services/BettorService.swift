import Foundation

class BettorService {
    func createBettor(appleSub: String, appleEmail: String?, appleName: String?) async throws -> Bettor {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = ["apple_sub": appleSub]
        if let email = appleEmail { body["apple_email"] = email }
        if let name = appleName   { body["apple_name"] = name }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, _) = try await URLSession.shared.data(for: request)
        return try JSONDecoder().decode(Bettor.self, from: data)
    }

    func signin(bettorId: Int) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor/signin") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: ["bettor_id": bettorId])
        let (_, response) = try await URLSession.shared.data(for: request)
        if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
            throw URLError(.badServerResponse)
        }
    }

    func updateProfile(bettorId: Int, profileName: String?, symbol: String?, color: String?) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor/\(bettorId)/profile") else {
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

        let (_, response) = try await URLSession.shared.data(for: request)
        if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
            throw URLError(.badServerResponse)
        }
    }
}
