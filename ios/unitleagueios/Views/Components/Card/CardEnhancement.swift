import SwiftUI

struct EnhancementTypeBadge: View {
    let type: String

    var badgeColor: Color {
        switch type {
        case "clv":  return .green
        case "team": return .yellow
        case "edge": return .red
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

struct EnhancementSymbolIcon: View {
    let type: String
    let symbol: String?

    var tintColor: Color {
        switch type {
        case "clv":  return .green
        case "team": return .yellow
        case "edge": return .red
        default:     return .secondary
        }
    }

    var body: some View {
        Image(systemName: symbol ?? "questionmark.circle")
            .font(.system(size: 16, weight: .semibold))
            .foregroundStyle(tintColor)
            .frame(width: 36, height: 36)
            .background(tintColor.opacity(0.15))
            .clipShape(RoundedRectangle(cornerRadius: 10))
    }
}

func enhancementRarityColor(_ rarity: String?) -> Color {
    switch rarity {
    case "dollar": return .gray
    case "nickel": return .green
    case "dime":   return .purple
    case "whale":  return .orange
    default:       return .secondary
    }
}

struct CardEnhancement: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let option: EnhanceOption
    var leagueName: String? = nil

    var body: some View {
        HStack(spacing: 12) {
            EnhancementSymbolIcon(type: option.enhancementType, symbol: option.symbol)

            VStack(alignment: .leading, spacing: 4) {
                Text(option.name)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
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

                if option.enhancementType == "edge" {
                    HStack(spacing: 6) {
                        Text(option.edgeType.capitalized)
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 3)
                            .background(Color.secondary.opacity(0.12))
                            .clipShape(Capsule())
                        if let rarity = option.rarity {
                            Text(rarity.capitalized)
                                .font(.caption2.weight(.bold))
                                .foregroundStyle(enhancementRarityColor(rarity))
                                .padding(.horizontal, 6)
                                .padding(.vertical, 3)
                                .background(enhancementRarityColor(rarity).opacity(0.15))
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
