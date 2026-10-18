import SwiftUI

struct MainTabView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId") private var bettorId: Int = 0
    @AppStorage("appleUserName") private var appleUserName: String = ""
    @State private var selectedTab = 2

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

            TabPicksView()
                .tabItem {
                    Label("Picks", systemImage: "pesetasign.bank.building.fill")
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
        .task { await refreshSession() }
    }

    /// Refreshes the session token on launch. With no valid token (an install from before API auth,
    /// or an expired/revoked session) the bettor is signed out locally so the Profile tab asks them
    /// to Sign in with Apple again; their profile comes back from the API.
    private func refreshSession() async {
        guard bettorId > 0 else {
            SessionToken.clear()  // Keychain outlives app deletion; drop a token from a previous install.
            return
        }
        if SessionToken.load() != nil {
            do {
                try await BettorService().signin()
                return
            } catch APIError.unauthorized {
                // Fall through to sign out.
            } catch {
                return  // Offline or server error: keep the session and try again next launch.
            }
        }
        bettorId = 0
        appleUserName = ""
    }
}


#Preview {
    MainTabView()
        .environmentObject(AppTheme())
        .environmentObject(BetStore())
}
