#if DEBUG
import SwiftUI

// MARK: - Syndicate debug init (no memberwise init due to custom Decodable)

extension Syndicate {
    init(
        syndicateId: Int,
        name: String,
        description: String? = nil,
        isPublic: Bool = true,
        maxRunner: Int? = nil,
        createdByBettorId: Int = 1,
        code: String? = nil,
        symbol: String? = nil,
        color: String? = nil,
        startUnits: Int? = nil,
        isStarted: Bool = false
    ) {
        self.syndicateId = syndicateId
        self.name = name
        self.description = description
        self.isPublic = isPublic
        self.maxRunner = maxRunner
        self.createdByBettorId = createdByBettorId
        self.code = code
        self.symbol = symbol
        self.color = color
        self.startUnits = startUnits
        self.isStarted = isStarted
    }
}

// MARK: - Mock data

enum Mock {

    // MARK: Leagues

    static let leagueNBA = League(
        id: 1, abbr: "NBA", name: "NBA", sport: "basketball",
        weather: "none", yrOrig: 1946, yrData: nil
    )
    static let leagueNFL = League(
        id: 2, abbr: "NFL", name: "NFL", sport: "football",
        weather: "outdoor", yrOrig: 1920, yrData: nil
    )

    // MARK: Teams

    static let teamLAL = Team(
        id: 1, leagueId: 1, abbr: "LAL", teamConcat: "LAL_NBA",
        name: "Lakers", location: "Los Angeles",
        conf: "West", div: "Pacific",
        lat: 34.04, lon: -118.26, weather: 0,
        mascot: "Lakers", color: "Purple", region: "West", category: "Person"
    )
    static let teamBOS = Team(
        id: 2, leagueId: 1, abbr: "BOS", teamConcat: "BOS_NBA",
        name: "Celtics", location: "Boston",
        conf: "East", div: "Atlantic",
        lat: 42.36, lon: -71.06, weather: 0,
        mascot: "Celtics", color: "Green", region: "East", category: "Person"
    )
    static let teamGSW = Team(
        id: 3, leagueId: 1, abbr: "GSW", teamConcat: "GSW_NBA",
        name: "Warriors", location: "San Francisco",
        conf: "West", div: "Pacific",
        lat: 37.77, lon: -122.39, weather: 0,
        mascot: "Warriors", color: "Gold", region: "West", category: "Person"
    )
    static var teamsNBA: [Team] { [teamLAL, teamBOS, teamGSW] }

    // MARK: Syndicates

    static let syndicate = Syndicate(
        syndicateId: 1, name: "Sharp Unit",
        description: "We find the edge.",
        isPublic: true, maxRunner: 10, createdByBettorId: 42,
        code: "SHARP7", symbol: "chart.line.uptrend.xyaxis", color: "Green"
    )
    static let syndicate2 = Syndicate(
        syndicateId: 2, name: "The Books",
        isPublic: false, maxRunner: nil, createdByBettorId: 99,
        code: "BOOK9", symbol: "book.fill", color: "Blue"
    )
    static var syndicates: [Syndicate] { [syndicate, syndicate2] }

    // MARK: Runners

    static let runnerAdmin = Runner(
        runnerId: 1, bettorId: 42, syndicateId: 1, role: "admin",
        active: true, balance: 250, profileName: "SharpQuinn",
        symbol: "person.fill", color: "Green"
    )
    static let runnerMember = Runner(
        runnerId: 2, bettorId: 77, syndicateId: 1, role: "member",
        active: true, balance: 180, profileName: "BetBot99",
        symbol: "bolt.fill", color: "Blue"
    )
    static var runners: [Runner] { [runnerAdmin, runnerMember] }

    // MARK: SelectedBet

    static let selectedBetML = SelectedBet(
        betHash: "ml_bos_lal_001",
        type: "ML", side: "Away",
        price: 2.10, points: nil,
        awayAbbr: "BOS", homeAbbr: "LAL",
        gameTime: "2026-06-10T23:30:00+00:00",
        gameDate: "2026-06-10"
    )
    static let selectedBetSPR = SelectedBet(
        betHash: "spr_bos_lal_001",
        type: "SPR", side: "Away",
        price: 1.91, points: 5.5,
        awayAbbr: "BOS", homeAbbr: "LAL",
        gameTime: "2026-06-10T23:30:00+00:00",
        gameDate: "2026-06-10"
    )
    static let selectedBetOU = SelectedBet(
        betHash: "ou_bos_lal_001",
        type: "O/U", side: "Over",
        price: 1.91, points: 224.5,
        awayAbbr: "BOS", homeAbbr: "LAL",
        gameTime: "2026-06-10T23:30:00+00:00",
        gameDate: "2026-06-10"
    )

    // MARK: PlacedBet

    static let placedBetML = PlacedBet(
        betHash: "ml_bos_lal_001",
        type: "ML", side: "Away",
        price: 2.10, points: nil, units: 2.0,
        awayAbbr: "BOS", homeAbbr: "LAL",
        gameTime: "2026-06-10T23:30:00+00:00",
        gameDate: "2026-06-10",
        bettorId: 42, syndicateId: 1
    )
    static let placedBetSPR = PlacedBet(
        betHash: "spr_bos_lal_001",
        type: "SPR", side: "Away",
        price: 1.91, points: 5.5, units: 1.0,
        awayAbbr: "BOS", homeAbbr: "LAL",
        gameTime: "2026-06-10T23:30:00+00:00",
        gameDate: "2026-06-10",
        bettorId: 42, syndicateId: 1
    )
    static var parlayLegs: [PlacedBet] {
        let groupId = UUID()
        return [
            PlacedBet(
                betHash: "ml_bos_lal_001", type: "ML", side: "Away",
                price: 2.10, points: nil, units: 0,
                awayAbbr: "BOS", homeAbbr: "LAL",
                gameTime: "2026-06-10T23:30:00+00:00", gameDate: "2026-06-10",
                bettorId: 42, syndicateId: 1, parlayGroupId: groupId
            ),
            PlacedBet(
                betHash: "spr_den_kc_001", type: "SPR", side: "Home",
                price: 1.91, points: -3.5, units: 0,
                awayAbbr: "DEN", homeAbbr: "KC",
                gameTime: "2026-06-10T23:30:00+00:00", gameDate: "2026-06-10",
                bettorId: 42, syndicateId: 1, parlayGroupId: groupId
            )
        ]
    }

    // MARK: Odds (decoded from JSON to satisfy all required fields)

    static let odds: Odds = {
        let json = """
        {
          "game_id": 101, "league_id": 1,
          "game_concat": "BOS_LAL_2026-06-10",
          "game_dt": "2026-06-10",
          "game_ts": "2026-06-10T23:30:00+00:00",
          "home_abbr": "LAL", "away_abbr": "BOS",
          "home_team_id": 1, "away_team_id": 2,
          "winner": null, "margin": null, "total": 224.5,
          "has_active_bets": false,
          "ml_home_bet_hash": "mlh001", "ml_home_bookmaker": "DraftKings",
          "ml_home_price": 1.74, "ml_home_won": null, "ml_home_bet_concat": "LAL_ML",
          "ml_away_bet_hash": "mla001", "ml_away_bookmaker": "FanDuel",
          "ml_away_price": 2.10, "ml_away_won": null, "ml_away_bet_concat": "BOS_ML",
          "spr_home_bet_hash": "sprh001", "spr_home_bookmaker": "DraftKings",
          "spr_home_price": 1.91, "spr_home_points": -5.5, "spr_home_won": null,
          "spr_home_bet_concat": "LAL_SPR",
          "spr_away_bet_hash": "spra001", "spr_away_bookmaker": "FanDuel",
          "spr_away_price": 1.91, "spr_away_points": 5.5, "spr_away_won": null,
          "spr_away_bet_concat": "BOS_SPR",
          "over_bet_hash": "ov001", "over_bookmaker": "BetMGM",
          "over_price": 1.91, "over_points": 224.5, "over_won": null,
          "over_bet_concat": "OVER_224.5",
          "under_bet_hash": "un001", "under_bookmaker": "Caesars",
          "under_price": 1.91, "under_points": 224.5, "under_won": null,
          "under_bet_concat": "UNDER_224.5",
          "last_updated_ts": "2026-06-10T14:00:00+00:00"
        }
        """
        return try! JSONDecoder().decode(Odds.self, from: Data(json.utf8))
    }()

    // MARK: OddMany

    static var oddMany: [OddMany] = {
        let json = """
        [
          {
            "bet_hash": "mla001", "bookmaker": "FanDuel",
            "game_id": 101, "league_id": 1,
            "game_dt": "2026-06-10", "game_ts": "2026-06-10T23:30:00+00:00",
            "home_abbr": "LAL", "away_abbr": "BOS",
            "team_id": null, "bet_type": "ML", "bet_concat": "BOS_ML",
            "price": 2.10, "points": null,
            "start_ts": "2026-06-09T12:00:00+00:00", "team_abbr": "BOS"
          },
          {
            "bet_hash": "mlh001", "bookmaker": "DraftKings",
            "game_id": 101, "league_id": 1,
            "game_dt": "2026-06-10", "game_ts": "2026-06-10T23:30:00+00:00",
            "home_abbr": "LAL", "away_abbr": "BOS",
            "team_id": null, "bet_type": "ML", "bet_concat": "LAL_ML",
            "price": 1.74, "points": null,
            "start_ts": "2026-06-09T12:00:00+00:00", "team_abbr": "LAL"
          },
          {
            "bet_hash": "spra001", "bookmaker": "FanDuel",
            "game_id": 101, "league_id": 1,
            "game_dt": "2026-06-10", "game_ts": "2026-06-10T23:30:00+00:00",
            "home_abbr": "LAL", "away_abbr": "BOS",
            "team_id": 2, "bet_type": "SPR", "bet_concat": "BOS_SPR",
            "price": 1.91, "points": 5.5,
            "start_ts": "2026-06-09T12:00:00+00:00", "team_abbr": "BOS"
          }
        ]
        """
        return try! JSONDecoder().decode([OddMany].self, from: Data(json.utf8))
    }()

    // MARK: Games

    static let gameLive = Game(
        id: 100, home: "LAL", away: "BOS",
        gameDate: "2026-06-08", gameTime: nil,
        homeScore: 104, awayScore: 112,
        winner: "BOS", leagueId: 1,
        homeTeamId: 1, awayTeamId: 2, wonTeamId: 2
    )
    static let gameUpcoming = Game(
        id: 101, home: "LAL", away: "BOS",
        gameDate: "2026-06-10", gameTime: "2026-06-10T23:30:00+00:00",
        homeScore: nil, awayScore: nil,
        winner: nil, leagueId: 1,
        homeTeamId: 1, awayTeamId: 2, wonTeamId: nil
    )
    static var games: [Game] { [gameLive, gameUpcoming] }

    // MARK: Txn (decoded from JSON — no memberwise init due to custom Decodable)

    static let txnML: Txn = {
        let json = """
        {
          "txn_id": 1, "bettor_id": 42, "syndicate_id": 1,
          "txn_type": "bet", "bet_hash": "ml_bos_lal_001", "parlay_id": null,
          "unit": 2.0, "price": 2.10, "won": null, "canceled": null,
          "bet_type": "ML", "points": null, "team": "BOS",
          "home": "LAL", "away": "BOS",
          "game_ts": "2026-06-10T23:30:00+00:00", "game_dt": "2026-06-10",
          "game_id": 101, "bookmaker": "FanDuel", "bet_concat": "BOS_ML"
        }
        """
        return try! JSONDecoder().decode(Txn.self, from: Data(json.utf8))
    }()

    static let txnSPR: Txn = {
        let json = """
        {
          "txn_id": 2, "bettor_id": 42, "syndicate_id": 1,
          "txn_type": "bet", "bet_hash": "spr_bos_lal_001", "parlay_id": null,
          "unit": 1.0, "price": 1.91, "won": null, "canceled": null,
          "bet_type": "SPR", "points": 5.5, "team": "BOS",
          "home": "LAL", "away": "BOS",
          "game_ts": "2026-06-10T23:30:00+00:00", "game_dt": "2026-06-10",
          "game_id": 101, "bookmaker": "FanDuel", "bet_concat": "BOS_SPR"
        }
        """
        return try! JSONDecoder().decode(Txn.self, from: Data(json.utf8))
    }()

    static let txnWon: Txn = {
        let json = """
        {
          "txn_id": 3, "bettor_id": 42, "syndicate_id": 1,
          "txn_type": "bet", "bet_hash": "ml_bos_lal_001", "parlay_id": null,
          "unit": 2.0, "price": 2.10, "won": true, "canceled": null,
          "bet_type": "ML", "points": null, "team": "BOS",
          "home": "LAL", "away": "BOS",
          "game_ts": "2026-06-08T23:30:00+00:00", "game_dt": "2026-06-08",
          "game_id": 100, "bookmaker": "FanDuel", "bet_concat": "BOS_ML"
        }
        """
        return try! JSONDecoder().decode(Txn.self, from: Data(json.utf8))
    }()

    static let txnLost: Txn = {
        let json = """
        {
          "txn_id": 4, "bettor_id": 42, "syndicate_id": 1,
          "txn_type": "bet", "bet_hash": "spr_lal_bos_002", "parlay_id": null,
          "unit": 1.5, "price": 1.91, "won": false, "canceled": null,
          "bet_type": "SPR", "points": -5.5, "team": "LAL",
          "home": "LAL", "away": "BOS",
          "game_ts": "2026-06-08T23:30:00+00:00", "game_dt": "2026-06-08",
          "game_id": 100, "bookmaker": "DraftKings", "bet_concat": "LAL_SPR"
        }
        """
        return try! JSONDecoder().decode(Txn.self, from: Data(json.utf8))
    }()

    static var txnParlay: [Txn] {
        let json = """
        [
          {
            "txn_id": 5, "bettor_id": 42, "syndicate_id": 1,
            "txn_type": "parlay", "bet_hash": "ml_bos_lal_001", "parlay_id": 99,
            "unit": 1.0, "price": 2.10, "won": null, "canceled": null,
            "bet_type": "ML", "points": null, "team": "BOS",
            "home": "LAL", "away": "BOS",
            "game_ts": "2026-06-10T23:30:00+00:00", "game_dt": "2026-06-10",
            "game_id": 101, "bookmaker": "FanDuel", "bet_concat": "BOS_ML",
            "parlay_price_mult": 4.0311
          },
          {
            "txn_id": 6, "bettor_id": 42, "syndicate_id": 1,
            "txn_type": "parlay", "bet_hash": "spr_den_kc_001", "parlay_id": 99,
            "unit": 0, "price": 1.91, "won": null, "canceled": null,
            "bet_type": "SPR", "points": -3.5, "team": "KC",
            "home": "KC", "away": "DEN",
            "game_ts": "2026-06-10T23:30:00+00:00", "game_dt": "2026-06-10",
            "game_id": 102, "bookmaker": "DraftKings", "bet_concat": "KC_SPR",
            "parlay_price_mult": 4.0311
          }
        ]
        """
        return try! JSONDecoder().decode([Txn].self, from: Data(json.utf8))
    }

    // MARK: Schedule

    static var schedItems: [Sched] {
        [
            Sched(
                id: "s1", teamAbbr: "LAL", oppAbbr: "BOS",
                gameDate: "2026-06-08", gameNum: 82, yr: 2026, schedConcat: nil,
                teamScore: 112, oppScore: 104, home: true, won: true,
                gameId: 100, leagueId: 1, teamId: 1, oppTeamId: 2
            ),
            Sched(
                id: "s2", teamAbbr: "LAL", oppAbbr: "GSW",
                gameDate: "2026-06-10", gameNum: 83, yr: 2026, schedConcat: nil,
                teamScore: nil, oppScore: nil, home: false, won: nil,
                gameId: 101, leagueId: 1, teamId: 1, oppTeamId: 3
            ),
            Sched(
                id: "s3", teamAbbr: "LAL", oppAbbr: "DEN",
                gameDate: "2026-06-01", gameNum: 81, yr: 2026, schedConcat: nil,
                teamScore: 98, oppScore: 105, home: true, won: false,
                gameId: 99, leagueId: 1, teamId: 1, oppTeamId: 4
            )
        ]
    }
}
#endif
