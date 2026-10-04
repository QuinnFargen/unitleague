import SwiftUI

/// Learn > Legal. Plain-language summary of what UNIT League is (and isn't) legally.
struct ViewLearnLegal: View {
    var body: some View {
        LearnPage(
            title: "Legal",
            lede: "What UNIT League is, what it isn't, and the fine print in plain language."
        ) {
            LearnHeading("Not a sportsbook")
            LearnText("UNIT League is a free-to-play game. It does not accept wagers, take deposits, or pay out winnings. You can never win or lose money on a pick.")
            LearnBullets([
                "**Units** have no cash value and can't be bought, sold, withdrawn, transferred, or exchanged for anything.",
                "Syndicate fees pay for access to the app, never for units, odds, or prizes.",
                "Standings are for bragging rights only.",
            ])

            LearnHeading("Odds and data")
            LearnText("Odds, lines, and scores come from third-party market data and are shown for entertainment and education. They may be delayed or inaccurate, and they aren't an offer to bet.")

            LearnHeading("Not advice")
            LearnText("Nothing in UNIT League is betting, financial, or legal advice. Results in the app don't predict how you'd do with real money at a sportsbook or prediction market.")

            LearnHeading("Real-money gambling")
            LearnText("Sports betting laws vary by state and country. Where it's legal, real-money sportsbooks generally require you to be **21+** (18+ in some places) and physically located in that state. Check your local laws before betting anywhere.")

            LearnHeading("Trademarks")
            LearnText("Team and league names and abbreviations are used only to identify real games. UNIT League isn't affiliated with or endorsed by any league, team, or sportsbook.")

            LearnCallout(
                "If gambling is affecting you or someone close to you, call or text the **National Problem Gambling Helpline** at [1-800-MY-RESET](tel:18006973738). It's free, confidential, and open 24/7.",
                "Your use of the app is also governed by our Terms of Service and Privacy Policy."
            )
        }
    }
}

#Preview {
    NavigationStack { ViewLearnLegal() }
        .environmentObject(AppTheme())
}
