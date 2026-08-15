import SwiftUI

/// Shared syndicate summary card. Used as a selectable list row in `TabSyndicateView`
/// (pass `isSelected:`) and as the detail-header banner in `ViewSyndicate` (pass `onEdit:`
/// when the current user is admin).
struct CardSyndicate: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let syndicate: Syndicate
    var showDescription: Bool = true
    var showCapsules: Bool = true
    var isSelected: Bool? = nil
    var onEdit: (() -> Void)? = nil

    private var selected: Bool { isSelected ?? false }

    var body: some View {
        let iconName = syndicate.symbol ?? (syndicate.isPublic ? "sportscourt" : "house.fill")
        let iconColor = ProfileOption.color(for: syndicate.color ?? "")

        return VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 16) {
                Image(systemName: iconName)
                    .font(.title2)
                    .foregroundStyle(iconColor)
                    .frame(width: 44, height: 44)
                    .background(selected ? theme.accent.opacity(0.15) : theme.cardBackground(colorScheme))
                    .clipShape(RoundedRectangle(cornerRadius: 10))

                VStack(alignment: .leading, spacing: 2) {
                    Text(syndicate.name)
                        .font(.headline)
                        .foregroundStyle(theme.primaryText(colorScheme))

                    if showDescription, let desc = syndicate.description, !desc.isEmpty {
                        Text(desc)
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
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

            if showCapsules {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 6) {
                        if let type = syndicate.syndicateType {
                            tagCapsule(type)
                        }
                        tagCapsule(syndicate.isPublic ? "Public" : "Private")
                        leagueCapsules
                        if let m = syndicate.maxRunner {
                            tagCapsule("\(m) losers")
                        }
                        if let su = syndicate.startUnits, su > 0 {
                            tagCapsule(icon: "nairasign.circle.fill", text: "\(su)")
                        }
                    }
                }
            }
        }
        .padding()
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

    @ViewBuilder
    private var leagueCapsules: some View {
        let ids = syndicate.leagueIds ?? []
        if ids.isEmpty {
            tagCapsule("All Leagues")
        } else {
            ForEach(ids, id: \.self) { id in
                tagCapsule(icon: League.sportIcon(for: id))
            }
        }
    }

    private func tagCapsule(_ text: String) -> some View {
        Text(text)
            .font(.caption2.weight(.semibold))
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .foregroundStyle(.secondary)
            .background(theme.cardBackgroundProminent(colorScheme))
            .clipShape(Capsule())
    }

    private func tagCapsule(icon: String, text: String? = nil) -> some View {
        HStack(spacing: 3) {
            Image(systemName: icon)
            if let text {
                Text(text)
            }
        }
        .font(.caption2.weight(.semibold))
        .padding(.horizontal, 6)
        .padding(.vertical, 2)
        .foregroundStyle(.secondary)
        .background(theme.cardBackgroundProminent(colorScheme))
        .clipShape(Capsule())
    }
}

#Preview("CardSyndicate") {
    VStack(spacing: 12) {
        CardSyndicate(syndicate: Mock.syndicate, isSelected: true)
        CardSyndicate(syndicate: Mock.syndicate2, isSelected: false)
        CardSyndicate(syndicate: Mock.syndicate, onEdit: {})
    }
    .padding()
    .environmentObject(AppTheme())
}
