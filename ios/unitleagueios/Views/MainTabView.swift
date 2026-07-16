import SwiftUI

struct MainTabView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId") private var bettorId: Int = 0
    @State private var selectedTab = 4

    var body: some View {
        TabView(selection: $selectedTab) {
            TabSyndicateView()
                .tabItem {
                    Label("Syndicate", systemImage: "person.3")
                }
                .tag(0)

            TabResearchView()
                .tabItem {
                    Label("Research", systemImage: "plus.forwardslash.minus")
                }
                .tag(1)

            TabBetsView()
                .tabItem {
                    Label("Bets", systemImage: "bitcoinsign.bank.building")
                }
                .tag(2)

            TabJuiceView()
                .tabItem {
                    Label("Juice", systemImage: "bolt.batteryblock.fill")
                }
                .tag(3)

            TabProfileView()
                .tabItem {
                    Label("Profile", systemImage: "figure.pickleball")
                }
                .tag(4)
        }
        .tint(theme.accent)
        .task {
            guard bettorId > 0 else { return }
            try? await BettorService().signin(bettorId: bettorId)
        }
    }
}


#Preview {
    MainTabView()
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
}
