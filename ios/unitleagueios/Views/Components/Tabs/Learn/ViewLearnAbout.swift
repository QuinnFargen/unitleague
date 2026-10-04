import SwiftUI

/// Learn > About Gambling. Mirrors `web/main/app/learn/about/page.js`.
struct ViewLearnAbout: View {
    var body: some View {
        LearnPage(
            title: "About Gambling",
            lede: "A fantasy-style betting league where the only thing on the line is bragging rights."
        ) {
            LearnText("UNIT League lets you and your friends make picks on real games, at real odds, and track who comes out ahead over a season. You never wager money, and you can never win or lose money on a pick. Everyone plays with **units** instead.")

            LearnHeading("Units, not dollars")
            LearnText("A **unit** is a standard bet size. Professional bettors think in units so their results compare across bankrolls. A “+12 unit season” means the same thing whether a unit is $1 or $1,000. UNIT League keeps the idea and drops the money:")
            LearnBullets([
                "Every runner in a **syndicate** starts with the same number of units.",
                "You stake units on **moneylines**, **spreads**, **totals**, and **parlays** at real market odds.",
                "Winning bets add units and losing bets subtract them. Most units at the end wins.",
                "Units can't be bought, sold, withdrawn, or exchanged for anything.",
            ])
            LearnText("You get the fun parts: the research, the sweat, the group chat trash talk. You don't get the part where a bad Sunday costs you rent. It also creates an honest record. Over a season you will see how hard it is to beat the **vig**, and how rarely parlays hit.")

            LearnHeading("What is a sportsbook?")
            LearnText("A **sportsbook** (or “book”) sets the odds and takes your bet directly. It is always the other side of your wager. Books don't need to predict games perfectly. They build a fee, the **vig**, into every price so that across millions of bets they keep a slice of the **handle** no matter who wins.")
            LearnText("Because the book is your opponent, it also controls the rules: which bets it takes, how much it lets you bet, and whether it keeps your account open.")

            LearnHeading("What is a prediction market?")
            LearnText("A **prediction market** is an **exchange**. Instead of betting against the house, you buy and sell **contracts** with other traders. A contract pays out 1 if an event happens and 0 if it doesn't, so a price of 0.62 means the market puts the chance at about 62%.")
            LearnBullets([
                "The exchange makes money from **fees** and **spreads** on every trade, not from you losing.",
                "Much of the action comes from **market makers**: firms that post buy and sell orders on both sides so there's always someone to trade with. The big sportsbooks are getting into prediction markets through this role, so the house can still end up on the other side of your trade.",
                "Winners generally aren't **limited** the way they are at books. Your opponent is another trader.",
                "It is still real money. Fees cut into every trade, and most traders still lose to better-informed ones.",
            ])

            LearnTable(
                header: ["", "Sportsbook", "Prediction market", "UNIT League"],
                rows: [
                    ["Who you bet against", "The house", "Other traders", "Not betting"],
                    ["How it makes money", "Vig and hold", "Trading fees", "Subscriptions & donations"],
                    ["Real money at risk", "Yes", "Yes", "No"],
                    ["Limits winners", "Yes", "Rarely", "Never"],
                ]
            )

            LearnCallout("Ready for the details? **Unit League** covers the syndicate types, how a season runs, and the Juice you can add to your picks.")
            NavigationLink {
                ViewLearnUnitLeague()
            } label: {
                NavCardRow(icon: LearnSection.unitLeague.icon, title: LearnSection.unitLeague.label)
            }
            .buttonStyle(.plain)
        }
    }
}

#Preview {
    NavigationStack { ViewLearnAbout() }
        .environmentObject(AppTheme())
}
