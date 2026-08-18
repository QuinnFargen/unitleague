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

// MARK: - Enhanced (Juice) rendering

/// A single team-level "juice" pill: name + level + currency icon. Used by `TabJuiceView` and
/// `SheetRunner`'s syndicate-scoped Juice section.
struct TeamLevelCapsule: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let item: Enhanced

    var body: some View {
        HStack(spacing: 6) {
            Text(item.name)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            Text("\(item.level)")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            Image(systemName: "nairasign.circle.fill")
                .font(.caption)
                .foregroundStyle(theme.accent)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(theme.cardBackground(colorScheme))
        .clipShape(Capsule())
    }
}

/// Three side-by-side CLV multiplier pills (ML/SPR/O-U). Used by `TabJuiceView` and
/// `SheetRunner`'s syndicate-scoped Juice section.
struct CLVLevelLine: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let items: [Enhanced]

    private static let order = ["ML", "SPR", "O/U"]

    private func level(for name: String) -> Int? {
        items.first { $0.name == name }?.level
    }

    private func multiplier(_ level: Int?) -> Double {
        guard let level, level > 0 else { return 1.0 }
        return Double(level) * 0.1 + 1.0
    }

    private func multiplierLabel(_ multiplier: Double) -> String {
        String(format: "%.1fx", multiplier)
    }

    var body: some View {
        HStack(spacing: 10) {
            ForEach(Self.order, id: \.self) { name in
                let mult = multiplier(level(for: name))
                HStack(spacing: 4) {
                    Text(name)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text(multiplierLabel(mult))
                        .font(.subheadline.weight(.bold))
                        .foregroundStyle(theme.primaryText(colorScheme))
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
                .background(theme.cardBackground(colorScheme))
                .clipShape(Capsule())
            }
        }
    }
}

/// A single "edge" enhancement row. Used by `TabJuiceView` and `SheetRunner`'s syndicate-scoped
/// Juice section.
struct EdgeEnhancementRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let item: Enhanced

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: item.symbol ?? "bolt.fill")
                .font(.subheadline)
                .foregroundStyle(theme.accent)
            Text(item.name)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
            Spacer()
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
