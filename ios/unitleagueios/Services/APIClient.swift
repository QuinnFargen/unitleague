import Foundation

enum APIClient {
    static let remoteURL = "https://api.unitleague.com"

    #if DEBUG
    static let localURL  = "http://192.168.4.59:8000"
    #endif

    static var baseURL: String {
        #if DEBUG
        if UserDefaults.standard.bool(forKey: "useLocalAPI") { return localURL }
        #endif
        return remoteURL
    }

    /// A JSON request carrying the signed-in bettor's session token. Use it for every write route.
    static func authorizedRequest(_ url: URL, method: String) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token = SessionToken.load() {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        return request
    }

    /// Sends an authorized request. A 401 means the session is gone (expired, revoked, or the
    /// account was deleted), so the stored token is dropped and `APIError.unauthorized` thrown.
    static func send(_ request: URLRequest) async throws -> (Data, URLResponse) {
        let (data, response) = try await URLSession.shared.data(for: request)
        if (response as? HTTPURLResponse)?.statusCode == 401 {
            SessionToken.clear()
            throw APIError.unauthorized
        }
        return (data, response)
    }
}

enum APIError: LocalizedError {
    case unauthorized

    var errorDescription: String? {
        switch self {
        case .unauthorized: return "Your session expired. Sign in again from the Profile tab."
        }
    }
}
