import SwiftUI

struct TabSyndicateView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("bettorId")            private var bettorId: Int = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int = 0

    @State private var syndicates: [Syndicate] = []
    @State private var isLoading = false
    @State private var fetchError: String?
    @State private var showingJoin = false
    @State private var showingCreate = false

    private let service = SyndicateService()

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 24) {
                        HStack(spacing: 12) {
                            LeagueActionButton(title: "Join", icon: "person.badge.plus", tint: theme.accent) {
                                showingJoin = true
                            }
                            LeagueActionButton(title: "Create", icon: "plus.circle", tint: theme.accent) {
                                showingCreate = true
                            }
                        }

                        if isLoading {
                            ProgressView().frame(maxWidth: .infinity).padding(.top, 40)
                        } else if let error = fetchError {
                            Text(error)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .frame(maxWidth: .infinity)
                                .padding(.top, 40)
                        } else if !syndicates.isEmpty {
                            VStack(alignment: .leading, spacing: 12) {
                                Text("Syndicates")
                                    .font(.headline)
                                    .foregroundStyle(.secondary)
                                    .frame(maxWidth: .infinity, alignment: .leading)

                                ForEach(syndicates) { syndicate in
                                    NavigationLink(destination: ViewSyndicate(syndicate: syndicate)) {
                                        CardSyndicate(
                                            syndicate: syndicate,
                                            isSelected: syndicate.syndicateId == selectedSyndicateId
                                        )
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                        } else {
                            Text("You're not in any syndicates yet.")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                                .frame(maxWidth: .infinity)
                                .padding(.top, 40)
                        }
                    }
                    .padding(.horizontal, 32)
                    .padding(.top, 16)
                    .padding(.bottom, 32)
                }
            }
            .tabToolbar()
            .task { await load() }
            .sheet(isPresented: $showingJoin, onDismiss: { Task { await load() } }) {
                SheetSyndicateJoin(bettorId: bettorId)
            }
            .sheet(isPresented: $showingCreate, onDismiss: { Task { await load() } }) {
                SheetSyndicateCreate(bettorId: bettorId)
            }
        }
    }

    private func load() async {
        guard bettorId > 0 else { return }
        isLoading = true
        fetchError = nil
        do {
            let raw = try await service.fetchSyndicate(bettorId: bettorId)
            var seen = Set<Int>()
            syndicates = raw.filter { seen.insert($0.syndicateId).inserted }
        } catch {
            fetchError = error.localizedDescription
        }
        isLoading = false
    }
}

// MARK: - Sub-views

private struct LeagueActionButton: View {
    let title: String
    let icon: String
    let tint: Color
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .font(.subheadline)
                .fontWeight(.semibold)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 14)
                .background(tint.opacity(0.15))
                .foregroundStyle(tint)
                .clipShape(RoundedRectangle(cornerRadius: 12))
                .overlay(RoundedRectangle(cornerRadius: 12).stroke(tint.opacity(0.35), lineWidth: 1))
        }
    }
}

enum SyndicateOption {
    static let symbols = [
        "person.3.fill",
        "house.fill",
        "bitcoinsign.bank.building.fill",
        "brain.filled.head.profile",
        "puzzlepiece.fill",
        "lock.fill"
    ]
}

#Preview {
    TabSyndicateView()
        .environmentObject(AppTheme())
}
