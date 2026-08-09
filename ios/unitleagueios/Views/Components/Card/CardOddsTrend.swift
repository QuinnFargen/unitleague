import SwiftUI

struct CardOddsTrend: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let record: TeamOddsRecent
    var season: TeamSeason? = nil

    private func pctLabel(_ pct: Double?) -> String {
        guard let pct else { return "—" }
        let s = String(format: "%.3f", pct)
        return s.hasPrefix("0.") ? String(s.dropFirst()) : s
    }

    private func mlMapping(_ ch: Character) -> (display: String, color: Color?) {
        switch ch {
        case "W": return ("W", theme.win)
        case "L": return ("L", theme.loss)
        case "T": return ("T", nil)
        default:  return (String(ch), nil)
        }
    }

    private func atsMapping(_ ch: Character) -> (display: String, color: Color?) {
        switch ch {
        case "W": return ("+", theme.win)
        case "L": return ("-", theme.loss)
        case "=": return ("=", nil)
        default:  return (String(ch), nil)
        }
    }

    private func ouMapping(_ ch: Character) -> (display: String, color: Color?) {
        switch ch {
        case "O": return ("O", theme.win)
        case "U": return ("U", theme.loss)
        case "=": return ("=", nil)
        default:  return (String(ch), nil)
        }
    }

    @ViewBuilder
    private func trendRow(_ label: String, pct: String, streak: String?, mapping: @escaping (Character) -> (display: String, color: Color?)) -> some View {
        HStack(spacing: 8) {
            Text(label)
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 26, alignment: .leading)
            Text(pct)
                .font(.caption.weight(.semibold).monospacedDigit())
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 36, alignment: .trailing)
            if let streak {
                TrendStreakText(streak: streak, mapping: mapping)
            }
        }
    }

    var body: some View {
        HStack(alignment: .center) {
            Text(record.abbr)
                .font(.headline)
                .foregroundStyle(theme.primaryText(colorScheme))
                .frame(width: 52, alignment: .leading)
                .lineLimit(1)

            Spacer()

            VStack(alignment: .leading, spacing: 4) {
                trendRow("WIN", pct: pctLabel(season?.winPct), streak: season?.last10Str, mapping: mlMapping)
                trendRow("ATS", pct: pctLabel(record.atsCoverPct), streak: record.atsLast10Str, mapping: atsMapping)
                trendRow("TOT", pct: pctLabel(record.overPct), streak: record.ouLast10Str, mapping: ouMapping)
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardOddsTrend") {
    VStack(spacing: 12) {
        CardOddsTrend(record: Mock.teamOddsRecentLAL, season: Mock.teamSeasonLAL)
        CardOddsTrend(record: Mock.teamOddsRecentBOS, season: Mock.teamSeasonBOS)
    }
    .padding()
    .environmentObject(AppTheme())
}
