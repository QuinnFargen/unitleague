import SwiftUI

// MARK: - GameOddsCard

struct CardGameOdds: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let odd: Odds
    let onBetSelected: (SelectedBet) -> Void

    private let colW: CGFloat = 58
    private let scoreW: CGFloat = 36

    private let timeInputFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withColonSeparatorInTimeZone]
        return f
    }()

    private let timeOutputFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "h:mm a"
        return f
    }()

    private var formattedTime: String? {
        guard let raw = odd.gameTime,
              let date = timeInputFormatter.date(from: raw) else { return nil }
        return timeOutputFormatter.string(from: date)
    }

    private var awayIsFav: Bool { (odd.sprAwayPoints ?? 1) < 0 }

    private var scores: (away: Int, home: Int)? {
        guard let margin = odd.margin, let total = odd.total, let winner = odd.winner else { return nil }
        let hi = (total + margin) / 2.0
        let lo = (total - margin) / 2.0
        if winner == odd.homeAbbr {
            return (away: Int(lo.rounded()), home: Int(hi.rounded()))
        } else {
            return (away: Int(hi.rounded()), home: Int(lo.rounded()))
        }
    }

    private func formatPrice(_ price: Double) -> String {
        String(format: "%.2f", price)
    }

    private func formatPoints(_ points: Double) -> String {
        points == points.rounded() ? "\(Int(points))" : String(format: "%.1f", points)
    }

    private func impliedPct(_ price: Double?) -> String {
        guard let p = price, p > 0 else { return "" }
        return "\(Int((1.0 / p * 100.0).rounded()))%"
    }

    private func oddsCapsuleColor(_ price: Double, betHash: String?, won: Bool?) -> Color {
        guard betHash != nil else { return theme.accent.opacity(0.2) }
        if let won { return won ? theme.accent.opacity(0.7) : theme.chipUnselected(colorScheme) }
        let distance = min(abs(price - 2.0) * 0.5, 0.85)
        let base = price < 2.0 ? theme.win : theme.loss
        return base.opacity(0.15 + distance)
    }

    @ViewBuilder
    private func scoreCapsule(_ value: Int) -> some View {
        Text("\(value)")
            .font(.caption.weight(.semibold))
            .foregroundStyle(theme.primaryText(colorScheme))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: scoreW)
            .background(theme.chipUnselected(colorScheme))
            .clipShape(Capsule())
    }

    @ViewBuilder
    private func priceCapsule(_ price: Double?, subtitle: String = "", betHash: String? = nil, won: Bool? = nil, displayOverride: String? = nil, onTap: (() -> Void)? = nil) -> some View {
        if let p = price {
            if betHash != nil, let onTap {
                Button(action: onTap) {
                    priceCapsuleLabel(p, display: displayOverride ?? formatPrice(p), subtitle: subtitle, betHash: betHash, won: won)
                }
                .buttonStyle(.plain)
            } else {
                priceCapsuleLabel(p, display: displayOverride ?? formatPrice(p), subtitle: subtitle, betHash: betHash, won: won)
            }
        } else {
            Text("—")
                .font(.caption)
                .foregroundStyle(.secondary)
                .frame(width: colW)
        }
    }

    @ViewBuilder
    private func priceCapsuleLabel(_ price: Double, display: String, subtitle: String, betHash: String?, won: Bool?) -> some View {
        VStack(spacing: 1) {
            Text(display)
                .font(.caption.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            if !subtitle.isEmpty {
                Text(subtitle)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .frame(width: colW)
        .background(oddsCapsuleColor(price, betHash: betHash, won: won))
        .clipShape(Capsule())
    }

    var body: some View {
        let ouTotal = (odd.overPoints ?? odd.underPoints).map(formatPoints) ?? ""
        let awayOUWon = awayIsFav ? odd.underWon : odd.overWon
        let homeOUWon = awayIsFav ? odd.overWon : odd.underWon
        let sprAwayLost = odd.sprAwayWon == false
        let sprHomeLost = odd.sprHomeWon == false

        HStack(alignment: .center, spacing: 10) {
            Image(systemName: odd.sportIcon)
                .font(.title)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 34)

            VStack(alignment: .leading, spacing: 6) {
                // Header row: time | spacer | score placeholder | ML | SPR | O/U
                HStack(spacing: 4) {
                    if let time = formattedTime {
                        Text(time)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    Spacer().frame(width: scoreW)
                    ForEach(["ML", "SPR", "O/U"], id: \.self) { h in
                        Text(h)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .frame(width: colW, alignment: .center)
                    }
                }

                // Away row
                HStack(spacing: 4) {
                    Text(odd.awayAbbr)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                        .frame(maxWidth: .infinity, alignment: .leading)
                    if let s = scores { scoreCapsule(s.away) } else { Spacer().frame(width: scoreW) }
                    priceCapsule(odd.mlAwayPrice, subtitle: impliedPct(odd.mlAwayPrice),
                                 betHash: odd.mlAwayBetHash, won: odd.mlAwayWon) {
                        guard let p = odd.mlAwayPrice, let h = odd.mlAwayBetHash else { return }
                        onBetSelected(SelectedBet(betHash: h, type: "ML", side: "Away", price: p, points: nil,
                                                  awayAbbr: odd.awayAbbr, homeAbbr: odd.homeAbbr,
                                                  gameTime: odd.gameTime, gameDate: odd.gameDt))
                    }
                    priceCapsule(odd.sprAwayPrice,
                                 subtitle: sprAwayLost ? "" : (odd.sprAwayPoints.map(formatPoints) ?? ""),
                                 betHash: odd.sprAwayBetHash, won: odd.sprAwayWon,
                                 displayOverride: sprAwayLost ? odd.margin.map(formatPoints) : nil) {
                        guard let p = odd.sprAwayPrice, let h = odd.sprAwayBetHash else { return }
                        onBetSelected(SelectedBet(betHash: h, type: "SPR", side: "Away", price: p, points: odd.sprAwayPoints,
                                                  awayAbbr: odd.awayAbbr, homeAbbr: odd.homeAbbr,
                                                  gameTime: odd.gameTime, gameDate: odd.gameDt))
                    }
                    priceCapsule(
                        awayIsFav ? odd.underPrice : odd.overPrice,
                        subtitle: awayOUWon == false ? "" : (awayIsFav ? "U \(ouTotal)" : "O \(ouTotal)"),
                        betHash: awayIsFav ? odd.underBetHash : odd.overBetHash,
                        won: awayOUWon,
                        displayOverride: awayOUWon == false ? odd.total.map(formatPoints) : nil
                    ) {
                        let price = awayIsFav ? odd.underPrice : odd.overPrice
                        let hash  = awayIsFav ? odd.underBetHash : odd.overBetHash
                        let pts   = odd.overPoints ?? odd.underPoints
                        guard let p = price, let h = hash else { return }
                        onBetSelected(SelectedBet(betHash: h, type: "O/U", side: awayIsFav ? "Under" : "Over",
                                                  price: p, points: pts, awayAbbr: odd.awayAbbr,
                                                  homeAbbr: odd.homeAbbr, gameTime: odd.gameTime, gameDate: odd.gameDt))
                    }
                }

                // Home row
                HStack(spacing: 4) {
                    Text("@ " + odd.homeAbbr)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                        .frame(maxWidth: .infinity, alignment: .leading)
                    if let s = scores { scoreCapsule(s.home) } else { Spacer().frame(width: scoreW) }
                    priceCapsule(odd.mlHomePrice, subtitle: impliedPct(odd.mlHomePrice),
                                 betHash: odd.mlHomeBetHash, won: odd.mlHomeWon) {
                        guard let p = odd.mlHomePrice, let h = odd.mlHomeBetHash else { return }
                        onBetSelected(SelectedBet(betHash: h, type: "ML", side: "Home", price: p, points: nil,
                                                  awayAbbr: odd.awayAbbr, homeAbbr: odd.homeAbbr,
                                                  gameTime: odd.gameTime, gameDate: odd.gameDt))
                    }
                    priceCapsule(odd.sprHomePrice,
                                 subtitle: sprHomeLost ? "" : (odd.sprHomePoints.map(formatPoints) ?? ""),
                                 betHash: odd.sprHomeBetHash, won: odd.sprHomeWon,
                                 displayOverride: sprHomeLost ? odd.margin.map(formatPoints) : nil) {
                        guard let p = odd.sprHomePrice, let h = odd.sprHomeBetHash else { return }
                        onBetSelected(SelectedBet(betHash: h, type: "SPR", side: "Home", price: p, points: odd.sprHomePoints,
                                                  awayAbbr: odd.awayAbbr, homeAbbr: odd.homeAbbr,
                                                  gameTime: odd.gameTime, gameDate: odd.gameDt))
                    }
                    priceCapsule(
                        awayIsFav ? odd.overPrice : odd.underPrice,
                        subtitle: homeOUWon == false ? "" : (awayIsFav ? "O \(ouTotal)" : "U \(ouTotal)"),
                        betHash: awayIsFav ? odd.overBetHash : odd.underBetHash,
                        won: homeOUWon,
                        displayOverride: homeOUWon == false ? odd.total.map(formatPoints) : nil
                    ) {
                        let price = awayIsFav ? odd.overPrice : odd.underPrice
                        let hash  = awayIsFav ? odd.overBetHash : odd.underBetHash
                        let pts   = odd.overPoints ?? odd.underPoints
                        guard let p = price, let h = hash else { return }
                        onBetSelected(SelectedBet(betHash: h, type: "O/U", side: awayIsFav ? "Over" : "Under",
                                                  price: p, points: pts, awayAbbr: odd.awayAbbr,
                                                  homeAbbr: odd.homeAbbr, gameTime: odd.gameTime, gameDate: odd.gameDt))
                    }
                }
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("GameOddsCard – upcoming") {
    CardGameOdds(odd: Mock.odds) { _ in }
        .padding()
        .environmentObject(AppTheme())
}
