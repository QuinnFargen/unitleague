import SwiftUI

/// Shared syndicate summary card.
/// - `.compact`: slim selectable list row in `TabSyndicateView` (pass `isSelected:`). The card is
///   the height of the syndicate symbol, with the league symbol/year capsule under the name.
/// - `.regular` / `.expanded`: detail-header banner in `ViewSyndicate` (pass `onEdit:` when the
///   current user is admin). `.expanded` draws the capsules larger; `ViewSyndicate` toggles it on tap.
struct CardSyndicate: View {
    enum Style { case compact, regular, expanded }

    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let syndicate: Syndicate
    var style: Style = .regular
    var isSelected: Bool? = nil
    var onEdit: (() -> Void)? = nil

    private var selected: Bool { isSelected ?? false }
    private var isCompact: Bool { style == .compact }
    private let symbolSize: CGFloat = 44

    var body: some View {
        let iconName = syndicate.symbol ?? (syndicate.isPublic ? "sportscourt" : "house.fill")
        let iconColor = ProfileOption.color(for: syndicate.color ?? "")

        return VStack(alignment: .leading, spacing: style == .expanded ? 16 : 12) {
            HStack(spacing: isCompact ? 12 : 16) {
                Image(systemName: iconName)
                    .font(.title2)
                    .foregroundStyle(iconColor)
                    .frame(width: symbolSize, height: symbolSize)
                    .background(selected ? theme.accent.opacity(0.15) : theme.cardBackground(colorScheme))
                    .clipShape(RoundedRectangle(cornerRadius: 10))

                if isCompact {
                    VStack(alignment: .leading, spacing: 3) {
                        Text(syndicate.name)
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                            .lineLimit(1)
                        leagueCapsule
                    }
                    .frame(maxHeight: symbolSize)
                } else {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(syndicate.name)
                            .font(.headline)
                            .foregroundStyle(theme.primaryText(colorScheme))

                        if let desc = syndicate.description, !desc.isEmpty {
                            Text(desc)
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                        }
                    }
                }

                Spacer()

                if let onEdit {
                    Button(action: onEdit) {
                        Image(systemName: "pencil.circle")
                            .font(.title3)
                            .foregroundStyle(theme.accent)
                    }
                } else if isSelected != nil {
                    if selected {
                        Image(systemName: "checkmark.circle.fill")
                            .font(.body)
                            .foregroundStyle(theme.accent)
                    } else {
                        Image(systemName: "chevron.right")
                            .font(.caption)
                            .foregroundStyle(.tertiary)
                    }
                }
            }

            if !isCompact {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: style == .expanded ? 10 : 6) {
                        leagueCapsule
                        if let type = syndicate.syndicateType {
                            tagCapsule(type)
                        }
                        tagCapsule(syndicate.isPublic ? "Public" : "Private")
                        if let m = syndicate.maxRunner {
                            tagCapsule(icon: "figure.pickleball.circle.fill", text: "\(m)")
                        }
                        if let su = syndicate.startUnits, su > 0 {
                            tagCapsule(icon: "nairasign.circle.fill", text: "\(su)")
                        }
                    }
                }
            }
        }
        .padding(.horizontal, isCompact ? 12 : 16)
        .padding(.vertical, isCompact ? 8 : 16)
        .background(
            selected
                ? LinearGradient(colors: [theme.accent.opacity(0.18), theme.cardBackground(colorScheme)], startPoint: .leading, endPoint: .trailing)
                : LinearGradient(colors: [theme.cardBackground(colorScheme)], startPoint: .leading, endPoint: .trailing)
        )
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .overlay(
            RoundedRectangle(cornerRadius: 14)
                .stroke(selected ? theme.accent.opacity(0.4) : Color.clear, lineWidth: 1.5)
        )
    }

    /// League symbol + season year, e.g. 🏈 2026.
    @ViewBuilder
    private var leagueCapsule: some View {
        if let leagueId = syndicate.leagueId {
            tagCapsule(icon: League.sportIcon(for: leagueId), text: syndicate.yr.map(String.init))
        }
    }

    private var capsuleFont: Font {
        style == .expanded ? .subheadline.weight(.semibold) : .caption2.weight(.semibold)
    }
    private var capsuleHPadding: CGFloat { style == .expanded ? 10 : 6 }
    private var capsuleVPadding: CGFloat { style == .expanded ? 5 : 2 }

    private func tagCapsule(_ text: String) -> some View {
        Text(text)
            .font(capsuleFont)
            .padding(.horizontal, capsuleHPadding)
            .padding(.vertical, capsuleVPadding)
            .foregroundStyle(.secondary)
            .background(theme.cardBackgroundProminent(colorScheme))
            .clipShape(Capsule())
    }

    private func tagCapsule(icon: String, text: String? = nil) -> some View {
        HStack(spacing: style == .expanded ? 5 : 3) {
            Image(systemName: icon)
            if let text {
                Text(text)
            }
        }
        .font(capsuleFont)
        .padding(.horizontal, capsuleHPadding)
        .padding(.vertical, capsuleVPadding)
        .foregroundStyle(.secondary)
        .background(theme.cardBackgroundProminent(colorScheme))
        .clipShape(Capsule())
    }
}

#if DEBUG
#Preview("CardSyndicate") {
    ScrollView {
        VStack(alignment: .leading, spacing: 12) {
            Text("Syndicate Tab").font(.headline).foregroundStyle(.secondary)
            CardSyndicate(syndicate: Mock.syndicate, style: .compact, isSelected: true)
            CardSyndicate(syndicate: Mock.syndicate2, style: .compact, isSelected: false)
            CardSyndicate(
                syndicate: Syndicate(syndicateId: 0, name: "Solo Loser", isPublic: false, symbol: "person.fill", color: "Green"),
                style: .compact,
                isSelected: false
            )

            Text("Syndicate Detail").font(.headline).foregroundStyle(.secondary).padding(.top, 12)
            CardSyndicate(syndicate: Mock.syndicate, onEdit: {})

            Text("Syndicate Detail – Tapped").font(.headline).foregroundStyle(.secondary).padding(.top, 12)
            CardSyndicate(syndicate: Mock.syndicate, style: .expanded, onEdit: {})
        }
        .padding()
    }
    .environmentObject(AppTheme())
}
#endif
