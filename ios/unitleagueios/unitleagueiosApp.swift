import SwiftUI

@main
struct unitleagueiosApp: App {
    @StateObject private var theme = AppTheme()
    @StateObject private var betStore = BetStore()

    var body: some Scene {
        WindowGroup {
            MainTabView()
                .environmentObject(theme)
                .environmentObject(betStore)
        }
    }
}
