import SwiftUI

/// Learn > Syndicate Types. The formats a syndicate can run, plus how you join one.
struct ViewLearnSyndicateTypes: View {
    private let formats: [(name: String, body: String, standings: String)] = [
        ("Unit",
         "Classic bankroll race. Everyone starts with the same units and bets them however they like over the season.",
         "Sorted by balance. Highest balance at the end of the season wins."),
        ("H2H",
         "Head-to-head like fantasy football. Each round you're matched against another runner, and whoever gains more units that round takes the win.",
         "Sorted by win–loss record, not total units."),
        ("Team",
         "Runners are grouped into teams, and team totals decide the standings. Good for offices, group chats, or rival friend groups.",
         "Sorted by combined team balance."),
    ]

    var body: some View {
        LearnPage(
            title: "Syndicate Types",
            lede: "Every syndicate picks a format. It decides how the standings are scored."
        ) {
            LearnHeading("Formats")
            ForEach(formats, id: \.name) { format in
                LearnHeading(format.name, level: 3)
                LearnText(format.body)
                LearnText("**Standings**: \(format.standings)")
            }

            LearnTable(
                header: ["", "Unit", "H2H", "Team"],
                rows: [
                    ["You compete against", "Everyone", "One runner per round", "Other teams"],
                    ["Standings by", "Balance", "Record", "Team balance"],
                    ["Best for", "Any group", "Fantasy fans", "Offices & big groups"],
                ]
            )

            LearnHeading("Public or private")
            LearnBullets([
                "**Public** syndicates can be joined by anyone before the admin starts the season.",
                "**Private** syndicates need the short code, plus a password if the admin set one.",
            ])

            LearnHeading("Solo")
            LearnText("Not in a syndicate? Your picks still count under **Solo Loser**, your personal league of one. It's a private record of how you'd do on your own.")

            LearnHeading("Sports leagues")
            LearnText("A syndicate can be limited to certain sports leagues (**NBA**, **NFL**, **NHL**, **MLB**, **CFB**, **CBB**). Leaving them all unselected allows every league.")

            LearnCallout("The format changes how you win, never what's at stake. Every syndicate type plays for units, and units have no cash value.")
        }
    }
}

#Preview {
    NavigationStack { ViewLearnSyndicateTypes() }
        .environmentObject(AppTheme())
}
