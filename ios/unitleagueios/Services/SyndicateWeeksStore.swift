import Foundation

/// Resolves a syndicate's active season(s) (via its `league_ids`) and fetches/caches the list
/// of weeks for those seasons, so completed-bet lists can be paged week-by-week without
/// refetching the season/week lookup on every view.
@MainActor
final class SyndicateWeeksStore: ObservableObject {
    @Published private(set) var weeks: [Week] = []

    private struct CacheEntry: Codable {
        let weeks: [Week]
        let cachedAt: Date
    }

    private let ttl: TimeInterval = 6 * 3600

    private func cacheKey(syndicateId: Int) -> String {
        "cachedWeeks_syndicate_\(syndicateId)"
    }

    private func loadCache(syndicateId: Int) -> CacheEntry? {
        guard let data = UserDefaults.standard.data(forKey: cacheKey(syndicateId: syndicateId)),
              let entry = try? JSONDecoder().decode(CacheEntry.self, from: data)
        else { return nil }
        return entry
    }

    private func saveCache(syndicateId: Int, weeks: [Week]) {
        let entry = CacheEntry(weeks: weeks, cachedAt: .now)
        guard let data = try? JSONEncoder().encode(entry) else { return }
        UserDefaults.standard.set(data, forKey: cacheKey(syndicateId: syndicateId))
    }

    /// `leagueIds`: pass the syndicate's known `league_ids` if already available (e.g. `ViewSyndicate`
    /// already has the `Syndicate` object); pass `nil` to have this fetch the `Syndicate` itself first
    /// (e.g. `SheetRunner`, which only has a `syndicateId`).
    func load(syndicateId: Int, leagueIds: [Int]?) async {
        if let cached = loadCache(syndicateId: syndicateId) {
            weeks = cached.weeks
            if Date().timeIntervalSince(cached.cachedAt) < ttl, !cached.weeks.isEmpty {
                return
            }
        }

        let ids: [Int]
        if let leagueIds {
            ids = leagueIds
        } else {
            let syndicates = (try? await SyndicateService().fetchSyndicate(syndicateId: syndicateId)) ?? []
            ids = syndicates.first?.leagueIds ?? []
        }
        guard !ids.isEmpty else { return }

        var seasonIds: [Int] = []
        for leagueId in ids {
            if let seasons = try? await SeasonService().fetchSeasons(leagueId: leagueId, active: true) {
                seasonIds.append(contentsOf: seasons.map(\.seasonId))
            }
        }
        guard !seasonIds.isEmpty else { return }

        var merged: [Week] = []
        for seasonId in seasonIds {
            if let fetched = try? await WeekService().fetchWeeks(seasonId: seasonId) {
                merged.append(contentsOf: fetched)
            }
        }
        merged.sort { $0.weekStartDt < $1.weekStartDt }

        weeks = merged
        saveCache(syndicateId: syndicateId, weeks: merged)
    }
}
