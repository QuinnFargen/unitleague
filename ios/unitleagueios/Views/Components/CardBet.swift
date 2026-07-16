import SwiftUI

struct CardBet: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let bet: SelectedBet

    private let timeInputFmt: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withColonSeparatorInTimeZone]
        return f
    }()
    private let timeOutputFmt: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "h:mm a"
        return f
    }()
    private let dateInputFmt: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()
    private let dateOutputFmt: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "EEE, MMM d"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private var formattedTime: String? {
        guard let raw = bet.gameTime, let d = timeInputFmt.date(from: raw) else { return nil }
        return timeOutputFmt.string(from: d)
    }

    private var formattedDate: String? {
        guard let raw = bet.gameDate, let d = dateInputFmt.date(from: raw) else { return nil }
        return dateOutputFmt.string(from: d)
    }

    private var betLabel: String {
        let teamSide = bet.team ?? (bet.side == "Away" ? bet.awayAbbr : (bet.side == "Home" ? bet.homeAbbr : bet.side))
        switch bet.type {
        case "SPR":
            if let p = bet.points {
                let s = p == p.rounded()
                    ? (p >= 0 ? "+\(Int(p))" : "\(Int(p))")
                    : String(format: p >= 0 ? "+%.1f" : "%.1f", p)
                return "\(teamSide) \(s)"
            }
            return "\(teamSide) SPR"
        case "O/U":
            if let p = bet.points {
                let s = p == p.rounded() ? "\(Int(p))" : String(format: "%.1f", p)
                return "\(teamSide) \(s)"
            }
            return "\(teamSide) O/U"
        default:
            return bet.type.isEmpty ? teamSide : "\(teamSide) \(bet.type)"
        }
    }

    var body: some View {
        HStack(spacing: 16) {
            VStack(alignment: .leading, spacing: 4) {
                Text(bet.awayAbbr + " @ " + bet.homeAbbr)
                    .font(.headline)
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let date = formattedDate {
                    Text(date)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                if let time = formattedTime {
                    Text(time)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 3) {
                Text(betLabel)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.secondary)
                HStack(alignment: .firstTextBaseline, spacing: 2) {
                    Text(String(format: "%.2f", bet.price))
                        .font(.title2.weight(.bold))
                        .foregroundStyle(theme.accent)
                    Text("x")
                        .font(.headline.weight(.bold))
                        .foregroundStyle(theme.accent)
                    if let u = bet.unit {
                        Text(txnWagerLabel(u))
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.secondary)
                        Image(systemName: "nairasign.circle.fill")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

#Preview("CardBet") {
    VStack(spacing: 12) {
        CardBet(bet: Mock.selectedBetML)
        CardBet(bet: Mock.selectedBetSPR)
        CardBet(bet: Mock.selectedBetOU)
    }
    .padding()
    .environmentObject(AppTheme())
}
