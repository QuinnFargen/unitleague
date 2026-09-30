import SwiftUI

/// Learn > FAQ. Mirrors `web/main/app/learn/faq/page.js`.
struct ViewLearnFAQ: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    private let faqs: [(q: String, a: String)] = [
        ("Is UNIT League gambling?",
         "No. You never wager or win money on a pick. Units have no cash value and can't be bought, sold, or withdrawn."),
        ("Does it cost anything?",
         "You can play on your own, or in one syndicate, for free. Joining more syndicates will cost a small $1 fee per runner. Fees never turn into units or winnings."),
        ("Where do the odds come from?",
         "We use real market odds from sportsbooks, so your picks face the same prices, including the **vig**, that a real bettor would."),
        ("How do I start or join a syndicate?",
         "Create one and share its code, or join a public syndicate. Private syndicates need the code and, if set, a password. See **Unit League**."),
        ("What's Juice?",
         "Enhancements you add to your runner to boost odds, stakes, or balance. Unlike sportsbook juice, ours works in your favor. See **Unit League**."),
        ("Will UNIT League help me win at a real sportsbook?",
         "That isn't the goal. It shows how hard winning is, and why books limit anyone who does win."),
        ("Who can play?",
         "See our Terms of Service for eligibility. Real-money sportsbooks require you to be 21+ in most US states."),
    ]

    var body: some View {
        LearnPage(
            title: "FAQ",
            lede: "Quick answers to common questions."
        ) {
            VStack(spacing: 0) {
                ForEach(Array(faqs.enumerated()), id: \.offset) { index, faq in
                    DisclosureGroup {
                        LearnText(faq.a)
                            .font(.subheadline)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.top, 8)
                    } label: {
                        Text(faq.q)
                            .font(.headline)
                            .foregroundStyle(theme.primaryText(colorScheme))
                            .multilineTextAlignment(.leading)
                    }
                    .tint(theme.accent)
                    .padding()
                    if index < faqs.count - 1 {
                        Divider().padding(.horizontal, 16)
                    }
                }
            }
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
        }
    }
}

#Preview {
    NavigationStack { ViewLearnFAQ() }
        .environmentObject(AppTheme())
}
