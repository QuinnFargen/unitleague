import SwiftUI

/// Learn > Unit League. Mirrors `web/main/app/learn/unit-league/page.js`.
struct ViewLearnUnitLeague: View {
    private let syndicateTypes: [(name: String, body: String)] = [
        ("Unit", "Classic bankroll race. Everyone starts with the same units, and the standings are sorted by balance. Highest balance at the end of the season wins."),
        ("H2H", "Head-to-head like fantasy football. Each round you're matched against another runner, and whoever gains more units that round takes the win. Standings are by record."),
        ("Team", "Runners are grouped into teams, and team totals decide the standings. Good for offices, group chats, or rival friend groups."),
    ]

    private let rarities: [[String]] = [
        ["Dollar", "1", "Cheap and common. Small, steady bonuses."],
        ["Nickel", "2", "Solid upgrades to odds, stake, or balance."],
        ["Dime", "3", "Stronger effects that often scale or have conditions."],
        ["Whale", "5", "Rare, build-defining effects, usually with a catch."],
    ]

    var body: some View {
        LearnPage(
            title: "Unit League",
            lede: "How syndicates, runners, and Juice fit together."
        ) {
            LearnHeading("Syndicates and runners")
            LearnText("A **syndicate** is your league. Whoever creates it sets the name, the starting **units**, the maximum number of players, and whether it's public or private (joined with a short code and optional password). Each player in a syndicate is a **runner**. You can run in several syndicates at once, and each one tracks its own balance.")
            LearnText("Once the admin starts the syndicate, runners place picks on real games at real market odds. Results settle automatically when the games finish.")

            LearnHeading("Syndicate types")
            ForEach(syndicateTypes, id: \.name) { type in
                LearnHeading(type.name, level: 3)
                LearnText(type.body)
            }

            LearnHeading("Syndicate settings")
            LearnText("Admins can tune a syndicate to their group:")
            LearnBullets([
                "**Minimum wagers / minimum units**: stop anyone from sitting on a lead by not betting.",
                "**Juice cost multiplier**: 2x, 3x, or 4x the cost of every enhancement.",
                "**Max active Edges**: cap how many Edge cards a runner holds at once.",
                "**Max CLV / Team level**: cap how far those enhancements can be stacked.",
                "**Blocked Juice**: turn off specific enhancements or whole categories.",
            ])

            LearnHeading("Juice")
            LearnCallout("At a sportsbook, **juice** is the fee that works against you. In UNIT League we flipped it: **Juice** is the set of enhancements that work *for* you.")
            LearnText("Each round you're offered a few Juice options and can add them to your runner. There are three kinds.")

            LearnHeading("CLV Juice", level: 3)
            LearnText("Named after **closing line value**. There is one for each bet type: **ML** (moneyline), **SPR** (spread), and **O/U** (totals). Leveling one up strengthens your bonus on that type of bet. CLV Juice is always on offer.")

            LearnHeading("Team Juice", level: 3)
            LearnText("Tie a bonus to teams that share a trait: **Color**, **Region**, **Mascot**, or **Conference**. You pick a team that matches the attribute you were offered. Choosing more Team Juice for the same team stacks its level.")

            LearnHeading("Edge Juice", level: 3)
            LearnText("Edges are cards with specific effects. Some add to the odds, some add stake or free units, and some change the rules of a round. They come in seven flavors: **price**, **unit**, **balance**, **trigger**, **risk**, **CLV**, and **team**. A few examples:")
            LearnBullets([
                "**Line Shopper** (price): +0.10 decimal odds on every bet.",
                "**Vig Cutter** (balance): losing bets refund 20% of stake.",
                "**Hot Hand** (trigger): +0.10 odds per consecutive win, resets on a loss.",
                "**Lock of the Day** (risk): one bet gets 2x odds, but costs 15 units if it loses.",
                "**Degenerate** (risk): 3x odds on everything, but you must stake half your balance every round.",
            ])
            LearnText("If you hit your syndicate's Edge limit, you sell one you hold to make room.")

            LearnHeading("Rarity and cost", level: 3)
            LearnText("Edges are ranked with betting slang for bet sizes: a “dollar” is $100, a “nickel” $500, and a “dime” $1,000. The rarer the Edge, the more it costs.")
            LearnTable(header: ["Rarity", "Base cost", "What to expect"], rows: rarities)

            LearnCallout("Real books have no Juice that helps you. Every “boost” they offer is priced to keep their edge.")
        }
    }
}

#Preview {
    NavigationStack { ViewLearnUnitLeague() }
        .environmentObject(AppTheme())
}
