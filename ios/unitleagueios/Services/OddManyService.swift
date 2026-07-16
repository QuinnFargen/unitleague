import Foundation

class OddManyService {
    func fetchOddAll(gameId: Int) async throws -> [OddMany] {
        var components = URLComponents(string: "\(APIClient.baseURL)/mart/game_oddall")!
        components.queryItems = [URLQueryItem(name: "game_id", value: "\(gameId)")]
        guard let url = components.url else { throw URLError(.badURL) }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([OddMany].self, from: data)
    }
}
