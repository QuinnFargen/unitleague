//
//  SchedCard.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 6/6/26.
//


import SwiftUI

struct CardSched: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let entry: Sched
    var isHighlighted: Bool = false

    private let dateInputFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private let dateOutputFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "EEE, MMM d"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private var formattedDate: String {
        guard let raw = entry.gameDate,
              let date = dateInputFormatter.date(from: raw) else { return "TBD" }
        return dateOutputFormatter.string(from: date)
    }

    private var matchup: String {
        entry.home ? "vs \(entry.oppAbbr ?? "TBD")" : "@ \(entry.oppAbbr ?? "TBD")"
    }

    private var isPlayed: Bool { entry.teamScore != nil && entry.oppScore != nil }

    var body: some View {
        if let gameId = entry.gameId {
            NavigationLink {
                ViewGameDetailLoader(gameId: gameId)
            } label: {
                cardBody
            }
            .buttonStyle(.plain)
        } else {
            cardBody
        }
    }

    private var cardBody: some View {
        HStack {
            if isPlayed {
                HStack(alignment: .firstTextBaseline, spacing: 6) {
                    Text(matchup)
                        .font(.headline)
                        .foregroundStyle(entry.won == true ? theme.win : theme.loss)
                    Text(formattedDate)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            } else {
                Text(matchup)
                    .font(.headline)
                    .foregroundStyle(theme.primaryText(colorScheme))
            }

            Spacer()

            if let teamScore = entry.teamScore, let oppScore = entry.oppScore {
                Text("\(teamScore) – \(oppScore)")
                    .font(.headline)
                    .foregroundStyle(
                        entry.won == true ? theme.win
                        : entry.won == false ? theme.loss
                        : theme.primaryText(colorScheme)
                    )
            } else {
                Text(formattedDate)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .overlay(
            RoundedRectangle(cornerRadius: 14)
                .stroke(theme.primaryText(colorScheme).opacity(0.6), lineWidth: 1.5)
                .opacity(isHighlighted ? 1 : 0)
        )
    }
}


#Preview("SchedCard") {
        // Winning
    CardSched(entry: Mock.schedItems[0], isHighlighted: true)
        .padding()
        .environmentObject(AppTheme())
        // Loss
    CardSched(entry: Mock.schedItems[2], isHighlighted: false)
        .padding()
        .environmentObject(AppTheme())
        // Upcoming
    CardSched(entry: Mock.schedItems[1], isHighlighted: false)
        .padding()
        .environmentObject(AppTheme())
}
