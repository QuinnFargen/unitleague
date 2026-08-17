import SwiftUI

/// League-then-team picker for the "Favorite Team" field in `SheetEditProfile`, mirroring the
/// league → `ViewTeamList(onSelect:)` flow already used by the team-enhancement picker in
/// `SheetAddJuice`.
struct SheetFavoriteTeamPicker: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    var onSelect: (Team) -> Void

    @State private var leagues: [League] = []
    @State private var isLoading = false

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                } else {
                    ScrollView {
                        VStack(spacing: 10) {
                            ForEach(leagues) { league in
                                NavigationLink {
                                    ViewTeamList(
                                        league: league,
                                        pickerTitle: "Favorite Team",
                                        onSelect: { team in
                                            onSelect(team)
                                            dismiss()
                                        }
                                    )
                                } label: {
                                    leagueRow(league)
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(16)
                    }
                }
            }
            .navigationTitle("Favorite Team")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
            .task {
                isLoading = true
                leagues = (try? await LeagueService().fetchLeagues()) ?? []
                isLoading = false
            }
        }
    }

    private func leagueRow(_ league: League) -> some View {
        HStack(spacing: 16) {
            Image(systemName: league.sportIcon)
                .font(.title2)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 44, height: 44)
                .background(theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 10))

            Text(league.abbr)
                .font(.title3.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))

            Spacer()

            Image(systemName: "chevron.right")
                .font(.caption)
                .foregroundStyle(.tertiary)
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("SheetFavoriteTeamPicker") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetFavoriteTeamPicker(onSelect: { _ in })
            .environmentObject(AppTheme())
    }
}
