import SwiftUI

struct CardGameOddSingle: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let odd: Odds
    let betType: String
    let onBetSelected: (SelectedBet) -> Void

    private let colW: CGFloat = 58

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

    private func oddsCapsuleColor(_ price: Double, betHash: String?, won: Bool?) -> Color {
        guard betHash != nil else { return theme.accent.opacity(0.2) }
        if let won { return won ? theme.accent.opacity(0.7) : theme.chipUnselected(colorScheme) }
        let distance = min(abs(price - 2.0) * 0.5, 0.85)
        let base = price < 2.0 ? theme.win : theme.loss
        return base.opacity(0.15 + distance)
    }

    @ViewBuilder
    private func priceCapsule(_ price: Double?, subtitle: String = "", betHash: String? = nil, won: Bool? = nil, onTap: (() -> Void)? = nil) -> some View {
        if let p = price {
            if betHash != nil, let onTap {
                Button(action: onTap) {
                    priceCapsuleLabel(p, subtitle: subtitle, betHash: betHash, won: won)
                }
                .buttonStyle(.plain)
            } else {
                priceCapsuleLabel(p, subtitle: subtitle, betHash: betHash, won: won)
            }
        } else {
            Text("—")
                .font(.caption)
                .foregroundStyle(.secondary)
                .frame(width: colW)
        }
    }

    @ViewBuilder
    private func priceCapsuleLabel(_ price: Double, subtitle: String, betHash: String?, won: Bool?) -> some View {
        VStack(spacing: 1) {
            Text(OddsFormatting.formatPrice(price))
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
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    var body: some View {
        singleModeLayout
            .padding()
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    private struct SingleData {
        let awayPrice: Double?
        let awayBetLabel: String
        let awayMLPct: String
        let awayBetHash: String?
        let awayWon: Bool?
        let awayPoints: Double?
        let awaySide: String
        let homePrice: Double?
        let homeBetLabel: String
        let homeMLPct: String
        let homeBetHash: String?
        let homeWon: Bool?
        let homePoints: Double?
        let homeSide: String
    }

    private func singleData() -> SingleData {
        switch betType {
        case "ML":
            return SingleData(
                awayPrice: odd.mlAwayPrice, awayBetLabel: "", awayMLPct: OddsFormatting.impliedPct(odd.mlAwayPrice),
                awayBetHash: odd.mlAwayBetHash, awayWon: odd.mlAwayWon, awayPoints: nil, awaySide: "Away",
                homePrice: odd.mlHomePrice, homeBetLabel: "", homeMLPct: OddsFormatting.impliedPct(odd.mlHomePrice),
                homeBetHash: odd.mlHomeBetHash, homeWon: odd.mlHomeWon, homePoints: nil, homeSide: "Home"
            )
        case "SPR":
            return SingleData(
                awayPrice: odd.sprAwayPrice,
                awayBetLabel: odd.sprAwayPoints.map(OddsFormatting.formatPointsSigned) ?? "",
                awayMLPct: OddsFormatting.impliedPct(odd.sprAwayPrice),
                awayBetHash: odd.sprAwayBetHash, awayWon: odd.sprAwayWon, awayPoints: odd.sprAwayPoints, awaySide: "Away",
                homePrice: odd.sprHomePrice,
                homeBetLabel: odd.sprHomePoints.map(OddsFormatting.formatPointsSigned) ?? "",
                homeMLPct: OddsFormatting.impliedPct(odd.sprHomePrice),
                homeBetHash: odd.sprHomeBetHash, homeWon: odd.sprHomeWon, homePoints: odd.sprHomePoints, homeSide: "Home"
            )
        case "O/U":
            let total = (odd.overPoints ?? odd.underPoints).map(OddsFormatting.formatPoints) ?? ""
            let pts = odd.overPoints ?? odd.underPoints
            return SingleData(
                awayPrice: odd.overPrice, awayBetLabel: "O \(total)", awayMLPct: OddsFormatting.impliedPct(odd.overPrice),
                awayBetHash: odd.overBetHash, awayWon: odd.overWon, awayPoints: pts, awaySide: "Over",
                homePrice: odd.underPrice, homeBetLabel: "U \(total)", homeMLPct: OddsFormatting.impliedPct(odd.underPrice),
                homeBetHash: odd.underBetHash, homeWon: odd.underWon, homePoints: pts, homeSide: "Under"
            )
        default:
            return SingleData(awayPrice: nil, awayBetLabel: "", awayMLPct: "", awayBetHash: nil, awayWon: nil, awayPoints: nil, awaySide: "",
                              homePrice: nil, homeBetLabel: "", homeMLPct: "", homeBetHash: nil, homeWon: nil, homePoints: nil, homeSide: "")
        }
    }

    @ViewBuilder
    private var singleModeLayout: some View {
        let d = singleData()
        HStack(spacing: 8) {
            NavigationLink {
                ViewGameDetail(
                    gameId: odd.gameId,
                    home: odd.homeAbbr,
                    away: odd.awayAbbr,
                    homeTeamId: odd.homeTeamId,
                    awayTeamId: odd.awayTeamId,
                    leagueId: odd.leagueId
                )
            } label: {
                Image(systemName: odd.sportIcon)
                    .font(.title2)
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .frame(width: 28)
            }
            .buttonStyle(.plain)

            priceCapsule(d.awayPrice, subtitle: d.awayMLPct, betHash: d.awayBetHash, won: d.awayWon) {
                guard let p = d.awayPrice, let h = d.awayBetHash else { return }
                onBetSelected(SelectedBet(betHash: h, type: betType, side: d.awaySide, price: p, points: d.awayPoints,
                                          awayAbbr: odd.awayAbbr, homeAbbr: odd.homeAbbr,
                                          gameTime: odd.gameTime, gameDate: odd.gameDt,
                                          homeTeamId: odd.homeTeamId, awayTeamId: odd.awayTeamId))
            }

            if !d.awayBetLabel.isEmpty {
                Text(d.awayBetLabel)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            VStack(spacing: 2) {
                Text(odd.awayAbbr + " @ " + odd.homeAbbr)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .multilineTextAlignment(.center)
                if let time = formattedTime {
                    Text(time).font(.caption2).foregroundStyle(.secondary)
                }
            }
            .frame(maxWidth: .infinity)

            if !d.homeBetLabel.isEmpty {
                Text(d.homeBetLabel)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            priceCapsule(d.homePrice, subtitle: d.homeMLPct, betHash: d.homeBetHash, won: d.homeWon) {
                guard let p = d.homePrice, let h = d.homeBetHash else { return }
                onBetSelected(SelectedBet(betHash: h, type: betType, side: d.homeSide, price: p, points: d.homePoints,
                                          awayAbbr: odd.awayAbbr, homeAbbr: odd.homeAbbr,
                                          gameTime: odd.gameTime, gameDate: odd.gameDt,
                                          homeTeamId: odd.homeTeamId, awayTeamId: odd.awayTeamId))
            }
        }
    }
}

#Preview("CardGameOddSingle") {
    VStack(spacing: 12) {
        CardGameOddSingle(odd: Mock.odds, betType: "ML") { _ in }
        CardGameOddSingle(odd: Mock.odds, betType: "SPR") { _ in }
        CardGameOddSingle(odd: Mock.odds, betType: "O/U") { _ in }
    }
    .padding()
    .environmentObject(AppTheme())
}
