import SwiftUI

struct CardRank: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let rank: Int
    let season: TeamSeason?
    var record: TeamOddsRecent? = nil

    private var abbr: String {
        season?.abbr ?? record?.abbr ?? "—"
    }

    private func pctLabel(_ pct: Double?) -> String {
        guard let pct else { return "—" }
        if pct >= 1.0 { return "1.00" }
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
        HStack(alignment: .center, spacing: 8) {
            Text("\(rank)")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 24, alignment: .leading)

            VStack(alignment: .leading, spacing: 2) {
                Text(abbr)
                    .font(.headline)
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .lineLimit(1)
                if let season {
                    Text("\(season.wins)-\(season.losses)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .frame(width: 60, alignment: .leading)

            Spacer(minLength: 8)

            VStack(alignment: .leading, spacing: 4) {
                trendRow("WIN", pct: pctLabel(season?.winPct), streak: season?.last10Str, mapping: mlMapping)
                if let record {
                    trendRow("ATS", pct: pctLabel(record.atsCoverPct), streak: record.atsLast10Str, mapping: atsMapping)
                    trendRow("TOT", pct: pctLabel(record.overPct), streak: record.ouLast10Str, mapping: ouMapping)
                }
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardRank") {
    VStack(spacing: 12) {
        CardRank(rank: 1, season: Mock.teamSeasonLAL)
        CardRank(rank: 2, season: Mock.teamSeasonBOS, record: Mock.teamOddsRecentBOS)
    }
    .padding()
    .environmentObject(AppTheme())
}
