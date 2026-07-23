import SwiftUI

// MARK: - AllOddsSection

struct CardOddMany: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let odds: [OddMany]
    let awayAbbr: String
    let homeAbbr: String
    let onBetSelected: (SelectedBet) -> Void

    @State private var isExpanded = false
    @State private var selectedBetType = "ML"

    private let colW: CGFloat = 62

    private func formatPrice(_ price: Double) -> String { String(format: "%.2f", price) }
    private func formatPoints(_ points: Double) -> String {
        points == points.rounded() ? "\(Int(points))" : String(format: "%.1f", points)
    }

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

    private var bookmakers: [String] {
        Array(Set(filteredOdds.map(\.bookmaker))).sorted()
    }

    @ViewBuilder
    private func oddsRow(bookmaker: String) -> some View {
        let lhsBets = filteredOdds.filter { $0.bookmaker == bookmaker && lhsMatch($0) }
        let rhsBets = filteredOdds.filter { $0.bookmaker == bookmaker && rhsMatch($0) }
        let lhs = lhsBets.first
        let rhs = rhsBets.first

        HStack(spacing: 8) {
            if let bet = lhs {
                Button { onBetSelected(selectedBet(from: bet)) } label: {
                    oddsLabel(bet)
                }
                .buttonStyle(.plain)
            } else {
                Text("—").font(.caption).foregroundStyle(.secondary).frame(width: colW)
            }

            Text(bookmaker)
                .font(.caption2)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .center)
                .lineLimit(1)

            if let bet = rhs {
                Button { onBetSelected(selectedBet(from: bet)) } label: {
                    oddsLabel(bet)
                }
                .buttonStyle(.plain)
            } else {
                Text("—").font(.caption).foregroundStyle(.secondary).frame(width: colW)
            }
        }
    }

    private func lhsMatch(_ odd: OddMany) -> Bool {
        switch selectedBetType {
        case "ML", "SPR": return odd.teamAbbr == awayAbbr
        case "O/U":        return odd.betType == "OVER"
        default:           return false
        }
    }

    private func rhsMatch(_ odd: OddMany) -> Bool {
        switch selectedBetType {
        case "ML", "SPR": return odd.teamAbbr == homeAbbr
        case "O/U":        return odd.betType == "UNDER"
        default:           return false
        }
    }

    @ViewBuilder
    private func oddsLabel(_ odd: OddMany) -> some View {
        VStack(spacing: 1) {
            Text(formatPrice(odd.price))
                .font(.caption.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            if let pts = odd.points {
                let prefix = odd.betType == "OVER" ? "O" : (odd.betType == "UNDER" ? "U" : "")
                let label = prefix.isEmpty ? formatPoints(pts) : "\(prefix) \(formatPoints(pts))"
                Text(label)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .frame(width: colW)
        .background(oddsCapsuleColor(odd.price))
        .clipShape(Capsule())
    }

    private var columnHeaders: (String, String) {
        switch selectedBetType {
        case "O/U": return ("Over", "Under")
        default:    return (awayAbbr, homeAbbr)
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            Button {
                withAnimation(.easeInOut(duration: 0.2)) { isExpanded.toggle() }
            } label: {
                HStack {
                    Text("All Odds")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                    Spacer()
                    Image(systemName: "chevron.down")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .rotationEffect(.degrees(isExpanded ? 0 : -90))
                }
                .padding(.horizontal, 14)
                .padding(.vertical, 12)
            }
            .buttonStyle(.plain)

            if isExpanded {
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

                    VStack(spacing: 6) {
                        ForEach(bookmakers, id: \.self) { bm in
                            oddsRow(bookmaker: bm)
                                .padding(.horizontal, 14)
                        }
                    }
                    .padding(.bottom, 12)
                }
            }
        }
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("AllOddsSection – collapsed") {
    CardOddMany(
        odds: Mock.oddMany,
        awayAbbr: "BOS",
        homeAbbr: "LAL"
    ) { _ in }
    .padding()
    .environmentObject(AppTheme())
}

