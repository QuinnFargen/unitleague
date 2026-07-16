import Foundation

final class BetStore: ObservableObject {
    @Published private(set) var bookmarks: [PlacedBet] = []

    private let bookmarkKey = "bookmarkedBets"

    init() {
        loadBookmarks()
    }

    func bookmark(_ bet: PlacedBet) {
        guard !bookmarks.contains(where: { $0.betHash == bet.betHash }) else { return }
        bookmarks.append(bet)
        persistBookmarks()
    }

    func removeBookmark(_ bet: PlacedBet) {
        bookmarks.removeAll { $0.betHash == bet.betHash }
        persistBookmarks()
    }

    func bookmarkParlay(_ legs: [PlacedBet]) {
        let groupId = UUID()
        for leg in legs {
            let tagged = PlacedBet(
                betHash: leg.betHash, type: leg.type, side: leg.side,
                price: leg.price, points: leg.points, units: leg.units,
                awayAbbr: leg.awayAbbr, homeAbbr: leg.homeAbbr,
                gameTime: leg.gameTime, gameDate: leg.gameDate,
                bettorId: leg.bettorId, syndicateId: leg.syndicateId,
                parlayGroupId: groupId
            )
            bookmarks.append(tagged)
        }
        persistBookmarks()
    }

    func removeBookmarkParlay(groupId: UUID) {
        bookmarks.removeAll { $0.parlayGroupId == groupId }
        persistBookmarks()
    }

    func clearBookmarks() {
        bookmarks = []
        persistBookmarks()
    }

    private func persistBookmarks() {
        if let data = try? JSONEncoder().encode(bookmarks) {
            UserDefaults.standard.set(data, forKey: bookmarkKey)
        }
    }

    private func loadBookmarks() {
        guard let data = UserDefaults.standard.data(forKey: bookmarkKey),
              let saved = try? JSONDecoder().decode([PlacedBet].self, from: data)
        else { return }
        bookmarks = saved
    }
}
