import SwiftUI

struct CardSyndicate: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let syndicate: Syndicate
    var isSelected: Bool = false

    var body: some View {
        let iconName = syndicate.symbol ?? (syndicate.isPublic ? "sportscourt" : "house.fill")
        let iconColor = ProfileOption.color(for: syndicate.color ?? "")

        return HStack(spacing: 16) {
            Image(systemName: iconName)
                .font(.title2)
                .foregroundStyle(iconColor)
                .frame(width: 44, height: 44)
                .background(isSelected ? theme.accent.opacity(0.15) : theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 10))

            VStack(alignment: .leading, spacing: 2) {
                Text(syndicate.name)
                    .font(.headline)
                    .foregroundStyle(theme.primaryText(colorScheme))

                if let desc = syndicate.description {
                    Text(desc)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                } else if syndicate.isPublic {
                    Text("Public")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            if isSelected {
                Image(systemName: "checkmark.circle.fill")
                    .font(.body)
                    .foregroundStyle(theme.accent)
            } else {
                Image(systemName: "chevron.right")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding()
        .background(
            isSelected
                ? LinearGradient(colors: [theme.accent.opacity(0.18), theme.cardBackground(colorScheme)], startPoint: .leading, endPoint: .trailing)
                : LinearGradient(colors: [theme.cardBackground(colorScheme)], startPoint: .leading, endPoint: .trailing)
        )
        .clipShape(RoundedRectangle(cornerRadius: 14))
        .overlay(
            RoundedRectangle(cornerRadius: 14)
                .stroke(isSelected ? theme.accent.opacity(0.4) : Color.clear, lineWidth: 1.5)
        )
    }
}

#Preview("CardSyndicate") {
    VStack(spacing: 12) {
        CardSyndicate(syndicate: Mock.syndicate, isSelected: true)
        CardSyndicate(syndicate: Mock.syndicate2, isSelected: false)
    }
    .padding()
    .environmentObject(AppTheme())
}
