import Foundation

class GameService {
    private let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    func fetchGames(date: Date, leagueId: Int?) async throws -> [Game] {
        let dateString = dateFormatter.string(from: date)
        var urlString = "\(APIClient.baseURL)/mart/game?game_dt=\(dateString)"
        if let id = leagueId {
            urlString += "&league_id=\(id)"
        }
        guard let url = URL(string: urlString) else {
            throw URLError(.badURL)
        }
        let (data, _) = try await URLSession.shared.data(from: url)
        return try JSONDecoder().decode([Game].self, from: data)
    }
}
