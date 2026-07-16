import Foundation


class LeagueService {
    func fetchLeagues() async throws -> [League] {
        guard let url = URL(string: "\(APIClient.baseURL)/mart/league") else {
            throw URLError(.badURL)
        }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([League].self, from: data)
    }
}
