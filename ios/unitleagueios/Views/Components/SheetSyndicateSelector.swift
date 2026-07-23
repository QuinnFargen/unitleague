//
//  SyndicateSelectorSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in SharedToolbar


import SwiftUI

struct SheetSyndicateSelector: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("profileSymbol") private var profileSymbol: String = ProfileOption.symbols[0]

    let bettorId: Int
    @Binding var selectedSyndicateId: Int
    @Binding var leagueSymbol: String
    @Binding var leagueColorName: String
    @Binding var leagueRank: Int

    @State private var syndicates: [Syndicate] = []
    @State private var myRunners: [Int: Runner] = [:]
    @State private var isLoading = false
    @State private var fetchError: String?

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                } else if let error = fetchError {
                    Text(error)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .padding()
                } else {
                    List {
                        Button {
                            selectedSyndicateId = 0
                            leagueSymbol = profileSymbol
                            leagueColorName = theme.accentOption.rawValue
                            leagueRank = 0
                            dismiss()
                        } label: {
                            HStack(spacing: 14) {
                                Image(systemName: profileSymbol)
                                    .font(.title2)
                                    .foregroundStyle(theme.accent)
                                    .frame(width: 40, height: 40)

                                Text("Solo Syndicate")
                                    .foregroundStyle(theme.primaryText(colorScheme))

                                Spacer()

                                if selectedSyndicateId == 0 {
                                    Image(systemName: "checkmark")
                                        .foregroundStyle(theme.accent)
                                }
                            }
                        }

                        ForEach(syndicates) { syndicate in
                            let iconName = syndicate.symbol ?? "person.3.fill"
                            let iconColor = ProfileOption.color(for: syndicate.color ?? "")
                            Button {
                                selectedSyndicateId = syndicate.syndicateId
                                leagueSymbol = iconName
                                leagueColorName = syndicate.color ?? AccentOption.allCases[0].rawValue
                                leagueRank = 0
                                dismiss()
                            } label: {
                                HStack(spacing: 14) {
                                    Image(systemName: iconName)
                                        .font(.title2)
                                        .foregroundStyle(iconColor)
                                        .frame(width: 40, height: 40)

                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(syndicate.name)
                                            .foregroundStyle(theme.primaryText(colorScheme))

                                        if let mine = myRunners[syndicate.syndicateId] {
                                            HStack(spacing: 4) {
                                                Image(systemName: mine.symbol ?? "person.fill")
                                                Text(mine.profileName ?? "")
                                            }
                                            .font(.caption2)
                                            .foregroundStyle(.secondary)
                                        }
                                    }

                                    Spacer()

                                    if selectedSyndicateId == syndicate.syndicateId {
                                        Image(systemName: "checkmark")
                                            .foregroundStyle(theme.accent)
                                    }
                                }
                            }
                        }
                    }
                    .listStyle(.plain)
                    .scrollContentBackground(.hidden)
                }
            }
            .navigationTitle("Select Syndicate")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
            .task { await load() }
        }
    }

    private func load() async {
        guard bettorId > 0 else { return }
        isLoading = true
        fetchError = nil
        do {
            async let syndicatesTask = SyndicateService().fetchSyndicate(bettorId: bettorId)
            async let runnersTask = RunnerService().fetchRunner(bettorId: bettorId)
            let raw = try await syndicatesTask
            var seen = Set<Int>()
            syndicates = raw.filter { seen.insert($0.syndicateId).inserted }
            let runners = (try? await runnersTask) ?? []
            myRunners = Dictionary(runners.map { ($0.syndicateId, $0) }, uniquingKeysWith: { first, _ in first })
        } catch {
            fetchError = error.localizedDescription
        }
        isLoading = false
    }
}

#Preview("SheetSyndicateSelector") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSyndicateSelector(
            bettorId: 0,
            selectedSyndicateId: .constant(1),
            leagueSymbol: .constant("chart.line.uptrend.xyaxis"),
            leagueColorName: .constant("Green"),
            leagueRank: .constant(2)
        )
        .environmentObject(AppTheme())
    }
}
