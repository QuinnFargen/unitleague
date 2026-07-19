import SwiftUI

struct EnhancementTypeBadge: View {
    let type: String

    var badgeColor: Color {
        switch type {
        case "clv":  return .blue
        case "team": return .green
        case "edge": return .orange
        default:     return .secondary
        }
    }

    var body: some View {
        Text(type.uppercased())
            .font(.system(size: 9, weight: .bold))
            .foregroundStyle(badgeColor)
            .padding(.horizontal, 6)
            .padding(.vertical, 3)
            .background(badgeColor.opacity(0.15))
            .clipShape(Capsule())
    }
}

struct CardEnhancement: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let option: EnhanceOption
    var leagueName: String? = nil

    var body: some View {
        HStack(spacing: 12) {
            EnhancementTypeBadge(type: option.enhancementType)

            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 6) {
                    Text(option.name)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                    if let betType = option.betType {
                        Text(betType.uppercased())
                            .font(.system(size: 9, weight: .medium))
                            .foregroundStyle(.secondary)
                            .padding(.horizontal, 5)
                            .padding(.vertical, 2)
                            .background(Color.secondary.opacity(0.12))
                            .clipShape(Capsule())
                    }
                }
                Text(option.description)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)

                if let value = option.availableAttrValue {
                    HStack(spacing: 6) {
                        Text(value)
                            .font(.caption.weight(.bold))
                            .foregroundStyle(.white)
                            .padding(.horizontal, 8)
                            .padding(.vertical, 3)
                            .background(theme.accent)
                            .clipShape(Capsule())
                        if let leagueName {
                            Text(leagueName)
                                .font(.caption2.weight(.semibold))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 6)
                                .padding(.vertical, 3)
                                .background(Color.secondary.opacity(0.12))
                                .clipShape(Capsule())
                        }
                    }
                    .padding(.top, 2)
                }
            }

            Spacer()

            Image(systemName: "chevron.right")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(12)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }
}

#Preview("CardEnhancement") {
    VStack(spacing: 12) {
        CardEnhancement(option: Mock.enhanceOptionCLV)
        CardEnhancement(option: Mock.enhanceOptionTeam, leagueName: "NBA")
        CardEnhancement(option: Mock.enhanceOptionEdge)
    }
    .padding()
    .environmentObject(AppTheme())
}
