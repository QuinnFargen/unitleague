import Foundation

class BettorService {
    /// Signs in with the `identityToken` from Sign in with Apple. The API verifies it with Apple,
    /// creates or loads the bettor for its `sub`, and returns a session token, stored in the Keychain.
    func createBettor(identityToken: String, appleEmail: String?, appleName: String?) async throws -> Bettor {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor") else {
            throw URLError(.badURL)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        var body: [String: Any] = ["identity_token": identityToken]
        if let email = appleEmail { body["apple_email"] = email }
        if let name = appleName   { body["apple_name"] = name }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await URLSession.shared.data(for: request)
        if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
            throw URLError(.userAuthenticationRequired)
        }
        let bettor = try JSONDecoder().decode(Bettor.self, from: data)
        guard let token = bettor.sessionToken else { throw URLError(.userAuthenticationRequired) }
        SessionToken.save(token)
        return bettor
    }

    /// Records the login and swaps the stored session token for a fresh one.
    /// Throws `APIError.unauthorized` (and drops the token) when the session is no longer valid.
    func signin() async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor/signin") else {
            throw URLError(.badURL)
        }
        let request = APIClient.authorizedRequest(url, method: "POST")
        let (data, response) = try await APIClient.send(request)
        if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
            throw URLError(.badServerResponse)
        }
        if let token = try JSONDecoder().decode(Bettor.self, from: data).sessionToken {
            SessionToken.save(token)
        }
    }

    func updateProfile(bettorId: Int, profileName: String?, symbol: String?, color: String?, appleEmail: String? = nil, favoriteTeamId: Int? = nil) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor/\(bettorId)/profile") else {
            throw URLError(.badURL)
        }
        var request = APIClient.authorizedRequest(url, method: "PATCH")

        var body: [String: Any] = [:]
        if let name = profileName         { body["profile_name"] = name }
        if let sym = symbol               { body["symbol"] = sym }
        if let col = color                { body["color"] = col }
        if let email = appleEmail         { body["apple_email"] = email }
        if let favoriteTeamId             { body["favorite_team_id"] = favoriteTeamId }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (_, response) = try await APIClient.send(request)
        if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
            throw URLError(.badServerResponse)
        }
    }

    /// Permanently deletes the bettor and their data. `authorizationCode` is a fresh Sign in
    /// with Apple code the API uses to revoke the app's Apple token.
    func deleteBettor(bettorId: Int, authorizationCode: String?) async throws {
        guard let url = URL(string: "\(APIClient.baseURL)/odd/bettor/\(bettorId)") else {
            throw URLError(.badURL)
        }
        var request = APIClient.authorizedRequest(url, method: "DELETE")

        var body: [String: Any] = [:]
        if let authorizationCode { body["authorization_code"] = authorizationCode }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (_, response) = try await APIClient.send(request)
        // 404 = already gone server-side; treat as deleted.
        if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode), http.statusCode != 404 {
            throw URLError(.badServerResponse)
        }
    }

    func fetchStats(bettorId: Int) async throws -> BettorStats? {
        guard var components = URLComponents(string: "\(APIClient.baseURL)/mart/bettor") else {
            throw URLError(.badURL)
        }
        components.queryItems = [URLQueryItem(name: "bettor_id", value: "\(bettorId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([BettorStats].self, from: data).first
    }

    func fetchLeagueBalances(bettorId: Int) async throws -> [BettorLeagueBalance] {
        guard var components = URLComponents(string: "\(APIClient.baseURL)/mart/bettor_league") else {
            throw URLError(.badURL)
        }
        components.queryItems = [URLQueryItem(name: "bettor_id", value: "\(bettorId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([BettorLeagueBalance].self, from: data)
    }
}
