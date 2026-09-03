import SwiftUI

// MARK: - AllOddsSection

struct CardOddMany: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let odds: [OddMany]
    let awayAbbr: String
    let homeAbbr: String
    let onBetSelected: (SelectedBet) -> Void

    @State private var selectedBetType = "ML"

    private let colW: CGFloat = 62

    private func oddsCapsuleColor(_ price: Double) -> Color {
        let distance = min(abs(price - 2.0) * 0.5, 0.85)
        let base = price < 2.0 ? theme.win : theme.loss
        return base.opacity(0.15 + distance)
    }

    private func selectedBet(from oddMany: OddMany) -> SelectedBet {
        let type = (oddMany.betType == "OVER" || oddMany.betType == "UNDER") ? "O/U" : oddMany.betType
        let side: String
        if oddMany.teamAbbr == nil {
            side = oddMany.betType == "OVER" ? "Over" : "Under"
        } else {
            side = oddMany.teamAbbr == awayAbbr ? "Away" : "Home"
        }
        return SelectedBet(betHash: oddMany.betHash, type: type, side: side,
                           price: oddMany.price, points: oddMany.points,
                           awayAbbr: oddMany.awayAbbr, homeAbbr: oddMany.homeAbbr,
                           gameTime: oddMany.gameTime, gameDate: oddMany.gameDt,
                           team: oddMany.teamAbbr)
    }

    private var filteredOdds: [OddMany] {
        switch selectedBetType {
        case "ML":   return odds.filter { $0.betType == "ML" }
        case "SPR":  return odds.filter { $0.betType == "SPR" }
        case "O/U":  return odds.filter { $0.betType == "OVER" || $0.betType == "UNDER" }
        default:     return []
        }
    }

    // MARK: ML — capsules per team, largest price on top

    private var mlAwayOdds: [OddMany] {
        filteredOdds.filter { $0.teamAbbr == awayAbbr }.sorted { $0.price > $1.price }
    }

    private var mlHomeOdds: [OddMany] {
        filteredOdds.filter { $0.teamAbbr == homeAbbr }.sorted { $0.price > $1.price }
    }

    // MARK: SPR — unique |spread| values, best price per side

    private var sprAbsValues: [Double] {
        Array(Set(filteredOdds.compactMap { $0.points.map(abs) })).sorted(by: >)
    }

    private func bestSprOdd(absPoints: Double, team: String) -> OddMany? {
        filteredOdds
            .filter { $0.teamAbbr == team && $0.points.map(abs) == absPoints }
            .max { $0.price < $1.price }
    }

    // MARK: O/U — unique total lines, best price per side

    private var ouTotals: [Double] {
        Array(Set(filteredOdds.compactMap(\.points))).sorted(by: >)
    }

    private func bestOuOdd(total: Double, side: String) -> OddMany? {
        filteredOdds
            .filter { $0.betType == side && $0.points == total }
            .max { $0.price < $1.price }
    }

    @ViewBuilder
    private func oddsSlot(_ odd: OddMany?) -> some View {
        if let odd {
            Button { onBetSelected(selectedBet(from: odd)) } label: {
                oddsLabel(odd)
            }
            .buttonStyle(.plain)
        } else {
            Text("—").font(.caption).foregroundStyle(.secondary).frame(width: colW)
        }
    }

    @ViewBuilder
    private var mlColumns: some View {
        HStack(alignment: .top, spacing: 8) {
            VStack(spacing: 6) {
                ForEach(mlAwayOdds) { odd in oddsSlot(odd) }
            }
            .frame(width: colW)
            Spacer()
            VStack(spacing: 6) {
                ForEach(mlHomeOdds) { odd in oddsSlot(odd) }
            }
            .frame(width: colW)
        }
    }

    @ViewBuilder
    private var sprRows: some View {
        VStack(spacing: 6) {
            ForEach(sprAbsValues, id: \.self) { value in
                HStack(spacing: 8) {
                    oddsSlot(bestSprOdd(absPoints: value, team: awayAbbr))
                    Spacer()
                    oddsSlot(bestSprOdd(absPoints: value, team: homeAbbr))
                }
            }
        }
    }

    @ViewBuilder
    private var ouRows: some View {
        VStack(spacing: 6) {
            ForEach(ouTotals, id: \.self) { total in
                HStack(spacing: 8) {
                    oddsSlot(bestOuOdd(total: total, side: "UNDER"))
                    Spacer()
                    oddsSlot(bestOuOdd(total: total, side: "OVER"))
                }
            }
        }
    }

    @ViewBuilder
    private func oddsLabel(_ odd: OddMany) -> some View {
        VStack(spacing: 1) {
            Text(OddsFormatting.formatPrice(odd.price))
                .font(.caption.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            if odd.betType == "ML" {
                Text(OddsFormatting.impliedPct(odd.price))
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            } else if let pts = odd.points {
                let label: String
                switch odd.betType {
                case "OVER":  label = "O \(OddsFormatting.formatPoints(pts))"
                case "UNDER": label = "U \(OddsFormatting.formatPoints(pts))"
                case "SPR":   label = OddsFormatting.formatPointsSigned(pts)
                default:      label = OddsFormatting.formatPoints(pts)
                }
                Text(label)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .frame(width: colW)
        .background(oddsCapsuleColor(odd.price))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    private var columnHeaders: (String, String) {
        switch selectedBetType {
        case "O/U": return ("Under", "Over")
        default:    return (awayAbbr, homeAbbr)
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Text("All Odds")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                Spacer()
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 12)

            Divider().background(theme.divider(colorScheme))

            VStack(spacing: 10) {
                HStack(spacing: 8) {
                    ForEach(["ML", "SPR", "O/U"], id: \.self) { t in
                        FilterChip(label: t, isSelected: selectedBetType == t) {
                            selectedBetType = t
                        }
                    }
                    Spacer()
                }
                .padding(.horizontal, 14)
                .padding(.top, 10)

                let (lhsLabel, rhsLabel) = columnHeaders
                HStack(spacing: 8) {
                    Text(lhsLabel)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: colW, alignment: .center)
                    Spacer()
                    Text(rhsLabel)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: colW, alignment: .center)
                }
                .padding(.horizontal, 14)

                Group {
                    switch selectedBetType {
                    case "ML":  mlColumns
                    case "SPR": sprRows
                    case "O/U": ouRows
                    default:    EmptyView()
                    }
                }
                .padding(.horizontal, 14)
                .padding(.bottom, 12)
            }
        }
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("AllOddsSection") {
    CardOddMany(
        odds: Mock.oddMany,
        awayAbbr: "BOS",
        homeAbbr: "LAL"
    ) { _ in }
    .padding()
    .environmentObject(AppTheme())
}
