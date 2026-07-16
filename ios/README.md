# Unit League iOS

A fantasy sports betting app built with SwiftUI. Users join syndicates, research teams, track games, and view odds across NFL, NBA, NHL, MLB, CFB, and CBB.

---

## Architecture

**Pattern:** Service-layer + MVVM-lite. Views manage their own `@State`, call async service functions, and receive theme via `@EnvironmentObject`.

```
unitleagueiosApp
├── AppTheme (EnvironmentObject)
└── MainTabView
    ├── TabSyndicateView
    ├── TabResearchView
    ├── TabGamesView
    ├── TabBetsView
    └── TabProfileView
```

**Persistence:** `@AppStorage` for user session (`bettorId`, profile settings). No local database — all data fetched from REST API (`APIClient`).

---

## Models

| File | Types | Used By |
|------|-------|---------|
| `Models/Game.swift` | `Game` | `TabGamesView`, `ViewGameDetail` |
| `Models/Odds.swift` | `Odds` | `TabBetsView`, `ViewGameDetail` |
| `Models/Team.swift` | `Team` (+ `teamColor`, `categoryIcon`, `regionIcon`) | `TabGamesView`, `ViewGameDetail`, `ViewTeamList`, `ViewTeamBanner` |
| `Models/League.swift` | `League` (+ `sportIcon(for:)`) | `TabResearchView`, `TabSyndicateView`, `ViewGameDetail` |
| `Models/Bettor.swift` | `Bettor` | `TabProfileView` |
| `Models/Sched.swift` | `Sched` | `ViewSched` |
| `Models/Runner.swift` | `Runner` | `ViewSyndicate` |
| `Models/Syndicate.swift` | `Syndicate` | `TabSyndicateView`, `ViewSyndicate` |

---

## Services

All services use `async/await` and hit the base URL defined in `Services/APIClient.swift`.

| File | Functions | Called By |
|------|-----------|-----------|
| `Services/APIClient.swift` | Base URL config | All services |
| `Services/GameService.swift` | `fetchGames(date:leagueId:)` | `TabGamesView` |
| `Services/OddsService.swift` | `fetchOddBest(gameId:gameDt:leagueId:)` | `TabBetsView`, `ViewGameDetail` |
| `Services/TeamService.swift` | `fetchTeams(leagueId:)` | `TabGamesView`, `ViewGameDetail`, `ViewTeamList` |
| `Services/LeagueService.swift` | `fetchLeagues()` | `TabResearchView`, `ViewGameDetail` |
| `Services/SchedService.swift` | `fetchSchedule(teamId:leagueId:yr:)` | `ViewSched` |
| `Services/BettorService.swift` | `createBettor(...)` `signin(bettorId:)` `updateProfile(...)` | `TabProfileView`, `MainTabView` |
| `Services/RunnerService.swift` | `fetchRunner(bettorId:syndicateId:)` | `ViewSyndicate`, `TabSyndicateView` |
| `Services/SyndicateService.swift` | `fetchSyndicate(syndicateId:bettorId:)` `createSyndicate(...)` `joinSyndicate(...)` | `TabSyndicateView` |

---

## Views

### App Shell

| File | Role |
|------|------|
| `unitleagueiosApp.swift` | Entry point — injects `AppTheme` environment object |
| `AppTheme.swift` | `ObservableObject` — accent colors, card/background colors, dark/light mode helpers |
| `Views/MainTabView.swift` | 5-tab container — calls `BettorService.signin()` on launch |
| `Views/SharedToolbar.swift` | Shared components: `FilterChip`, `DateNavigationHeader`, `TabToolbar` view modifier |

### Tab Views

| File | Purpose | Models Used | Services Used | Navigates To |
|------|---------|-------------|---------------|--------------|
| `Views/Tabs/TabGamesView.swift` | Browse games by date and league | `Game`, `Team` | `GameService`, `TeamService` | `ViewGameDetail` |
| `Views/Tabs/TabBetsView.swift` | Browse best odds by date, league, bet type | `Odds` | `OddsService` | `ViewGameDetail` |
| `Views/Tabs/TabResearchView.swift` | Explore leagues → teams → schedules | `League` | `LeagueService` | `ViewTeamList` |
| `Views/Tabs/TabSyndicateView.swift` | Manage syndicates (view/join/create) | `Syndicate`, `Runner` | `SyndicateService`, `RunnerService` | `ViewSyndicate` |
| `Views/Tabs/TabProfileView.swift` | Apple Sign-In, profile name/symbol/color | `Bettor` | `BettorService` | — |

### Component Views

| File | Purpose | Models Used | Services Used | Navigates To |
|------|---------|-------------|---------------|--------------|
| `Views/Components/ViewGameDetail.swift` | Full game page with odds table and team banners | `Odds`, `Team`, `League` | `OddsService`, `TeamService`, `LeagueService` | `ViewSched` |
| `Views/Components/ViewTeamList.swift` | Teams in a league filtered by conference/division | `Team` | `TeamService` | `ViewSched` |
| `Views/Components/ViewSched.swift` | Team schedule with year picker and recent mode | `Sched` | `SchedService` | — |
| `Views/Components/ViewSyndicate.swift` | Syndicate leaderboard ranked by runner balance | `Runner` | `RunnerService` | — |
| `Views/Components/ViewTeamBanner.swift` | Reusable team header card (gradient, meta row) | `Team` | — | — |

---

## Data Flow

### Auth
```
TabProfileView → Apple Sign-In → BettorService.createBettor() → @AppStorage bettorId
MainTabView (on launch) → BettorService.signin(bettorId)
```

### Games & Odds
```
TabGamesView → GameService.fetchGames(date, leagueId)
             → GameCard → ViewGameDetail
                          → OddsService.fetchOddBest()  (concurrent)
                          → TeamService.fetchTeams()     (concurrent)
                          → LeagueService.fetchLeagues() (concurrent)
                          → ViewSched (per team)

TabBetsView  → OddsService.fetchOddBest(date, leagueId, betType)
             → OddBestCard → ViewGameDetail
```

### Research
```
TabResearchView → LeagueService.fetchLeagues()
                → ViewTeamList → TeamService.fetchTeams(leagueId)
                               → ViewTeamBanner + ViewSched
                                 → SchedService.fetchSchedule(teamId, yr)
```

### Syndicates
```
TabSyndicateView → SyndicateService.fetchSyndicate(bettorId)
                 → SyndicateCard → ViewSyndicate
                                   → RunnerService.fetchRunner(syndicateId)
                 → JoinSyndicateSheet  → SyndicateService.joinSyndicate()
                 → CreateSyndicateSheet → SyndicateService.createSyndicate()
```

---

## State Management

| Mechanism | Used For |
|-----------|----------|
| `@EnvironmentObject AppTheme` | Colors, accent, dark/light mode — available to all views |
| `@AppStorage` | `bettorId`, profile symbol/color, league preferences — persisted across launches |
| `@State` | Local loading, error, selection, and form state within each view |
| `@Environment(\.colorScheme)` | Adaptive light/dark styling within `AppTheme` helpers |

---

## League IDs

Defined in `League.sportIcon(for:)` and used throughout for filtering and icons.

| ID | League |
|----|--------|
| 1 | NBA |
| 2 | NFL |
| 3 | NHL |
| 4 | MLB |
| 5 | CFB |
| 6 | CBB |
