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

    let bettorId: Int
    @Binding var selectedSyndicateId: Int
    @Binding var leagueSymbol: String
    @Binding var leagueColorName: String
    @Binding var leagueRank: Int

    @State private var syndicates: [Syndicate] = []
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
                            leagueSymbol = "person.circle.fill"
                            leagueColorName = AccentOption.allCases[0].rawValue
                            leagueRank = 0
                            dismiss()
                        } label: {
                            HStack(spacing: 14) {
                                Image(systemName: "person.circle.fill")
                                    .font(.title2)
                                    .foregroundStyle(.secondary)
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

                                    Text(syndicate.name)
                                        .foregroundStyle(theme.primaryText(colorScheme))

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
            let raw = try await SyndicateService().fetchSyndicate(bettorId: bettorId)
            var seen = Set<Int>()
            syndicates = raw.filter { seen.insert($0.syndicateId).inserted }
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
