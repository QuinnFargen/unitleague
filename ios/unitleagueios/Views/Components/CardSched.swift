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
        f.dateFormat = "EEE, MMM d, yyyy"
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

    var body: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text(matchup)
                    .font(.headline)
                    .foregroundStyle(entry.teamScore != nil && entry.oppScore != nil
                        ? (entry.won == true ? theme.win : theme.loss)
                        : theme.primaryText(colorScheme))
                Text(formattedDate)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            if let teamScore = entry.teamScore, let oppScore = entry.oppScore {
                VStack(alignment: .trailing, spacing: 2) {
                    Text("\(teamScore) – \(oppScore)")
                        .font(.headline)
                        .foregroundStyle(theme.primaryText(colorScheme))
                    if let won = entry.won {
                        Text(won ? "W · FINAL" : "L · FINAL")
                            .font(.caption2)
                            .foregroundStyle(won ? theme.win : theme.loss)
                    }
                }
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
