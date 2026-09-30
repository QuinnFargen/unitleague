import SwiftUI

/// Learn > Resource. Mirrors `web/main/app/learn/resource/page.js`.
/// Videos open in YouTube rather than embedding a player.
struct ViewLearnResource: View {
    var body: some View {
        LearnPage(
            title: "Resource",
            lede: "Worth your time before you put real money on a game."
        ) {
            LearnHeading("Addiction")
            LearnCallout(
                "**National Problem Gambling Helpline**: 24/7/365 confidential support and referrals. Call or text [1-800-MY-RESET](tel:18006973738).",
                "**Gamblers Anonymous (GA)**: recovery meetings. Call [1-855-222-5542](tel:18552225542)."
            )
            LearnText("If gambling is affecting you or someone close to you, help is free and confidential. The [National Council on Problem Gambling](https://www.ncpgambling.org/help-treatment/) lists treatment options, support groups, and state resources.")

            LearnHeading("Dangers")
            LearnText("[Breaking Points](https://www.youtube.com/@breakingpoints) is an independent news show that isn't paid by the gambling industry. That's rare: nearly every sports podcast and many national news outlets now run sportsbook or prediction market ads, which makes it hard for them to cover the harm honestly.")
            VideoRow(
                id: "jSxZUw923gs",
                title: "Former FanDuel CEO Admits Ads Are A LIE",
                note: "A former industry insider on what sportsbook advertising leaves out."
            )
            VideoRow(
                id: "qJw7lIO9KeE",
                title: "Why Online Gambling Is The Next Opioid Crisis",
                note: "How mobile betting went mainstream, and the addiction it's driving."
            )

            LearnHeading("Financial")
            LearnText("[The Money Guy Show](https://www.youtube.com/@MoneyGuyShow) covers personal finance. **Is your Roth IRA funded?** If not, that money has a better job than a bet slip.")
            LearnBullets([
                "[Financial Order of Operations](https://moneyguy.com/resource/financial-order-of-operations/): their step-by-step plan for what to do with each dollar.",
                "[Money Guy resources](https://moneyguy.com/resources/): free guides, checklists, and tools.",
            ])
            LearnText("Watch the **first 20 minutes** of this video to learn where sports betting should fall in your personal finance priorities.")
            VideoRow(
                id: "7LhSFW09tzs",
                title: "How To Actually Make Money Sports Betting (Here's the Math)"
            )

            LearnHeading("Math")
            LearnBullets([
                "[gambling-math.com](https://gambling-math.com): calculators and plain-language explanations of odds, house edge, and why betting systems fail.",
                "[Siena Research Institute poll (April 2026)](https://sri.siena.edu/2026/04/13/more-than-a-quarter-of-americans-27-have-an-active-online-sports-betting-account-a-third-have-opened-an-account-at-least-once/): statistics on who bets and what it costs them. More than a quarter of Americans (27%) have an active online sports betting account, and a third have opened one at least once.",
            ])
        }
    }
}

/// Tappable card that opens a YouTube video.
private struct VideoRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let id: String
    let title: String
    var note: String? = nil

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let note {
                LearnText(note)
            }
            Link(destination: URL(string: "https://www.youtube.com/watch?v=\(id)")!) {
                HStack(spacing: 12) {
                    Image(systemName: "play.rectangle.fill")
                        .font(.title2)
                        .foregroundStyle(theme.accent)
                    Text(title)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                        .multilineTextAlignment(.leading)
                    Spacer()
                    Image(systemName: "arrow.up.right")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .padding()
                .background(theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 14))
            }
        }
    }
}

#Preview {
    NavigationStack { ViewLearnResource() }
        .environmentObject(AppTheme())
}
