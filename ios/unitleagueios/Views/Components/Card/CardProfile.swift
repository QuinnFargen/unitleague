import SwiftUI

/// Shared profile header card: symbol + name, an optional favorite-team capsule, and an
/// optional career unit-history value (+100 / -46 style). Used by `TabProfileView` for the
/// signed-in bettor (editable) and `SheetRunner` for another syndicate runner (read-only).
struct CardProfile: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let symbol: String?
    let color: String?
    let name: String
    var favoriteTeamAbbr: String? = nil
    var favoriteLeagueId: Int? = nil
    var careerUnits: Double? = nil
    var isEditable: Bool = false
    var onEdit: (() -> Void)? = nil
    /// Shown instead of the edit pencil when `isEditable` is false — e.g. a link to view this
    /// runner's full cross-syndicate profile when they're an opponent, not the signed-in bettor.
    var showsViewProfileButton: Bool = false
    var onViewProfile: (() -> Void)? = nil

    private var iconColor: Color {
        color.map { ProfileOption.color(for: $0) } ?? theme.accent
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .center, spacing: 14) {
                Image(systemName: symbol ?? "person.crop.circle.fill")
                    .font(.system(size: 28, weight: .semibold))
                    .foregroundStyle(iconColor)
                    .frame(width: 48, height: 48)
                    .background(theme.cardBackgroundProminent(colorScheme))
                    .clipShape(RoundedRectangle(cornerRadius: 12))

                Text(name)
                    .font(.title2).bold()
                    .foregroundStyle(theme.primaryText(colorScheme))

                Spacer()

                if isEditable {
                    Button {
                        onEdit?()
                    } label: {
                        Image(systemName: "pencil.circle")
                            .font(.title3)
                            .foregroundStyle(theme.accent)
                    }
                    .buttonStyle(.plain)
                } else if showsViewProfileButton {
                    Button {
                        onViewProfile?()
                    } label: {
                        Image(systemName: "figure.pickleball.circle.fill")
                            .font(.title3)
                            .foregroundStyle(theme.accent)
                    }
                    .buttonStyle(.plain)
                }
            }

            if favoriteTeamAbbr != nil || careerUnits != nil {
                HStack(spacing: 8) {
                    if let favoriteTeamAbbr, let favoriteLeagueId {
                        tagCapsule(icon: League.sportIcon(for: favoriteLeagueId), text: favoriteTeamAbbr)
                    }

                    Spacer()

                    if let careerUnits {
                        unitHistoryValue(careerUnits)
                    }
                }
            }
        }
        .padding()
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    private func unitHistoryValue(_ value: Double) -> some View {
        let tint = value >= 0 ? theme.win : theme.loss
        let sign = value >= 0 ? "+" : ""
        return HStack(spacing: 3) {
            Text("\(sign)\(txnWagerLabel(value))")
                .font(.subheadline.weight(.semibold))
            Image(systemName: "nairasign.circle.fill")
        }
        .foregroundStyle(tint)
    }

    private func tagCapsule(icon: String, text: String) -> some View {
        HStack(spacing: 3) {
            Image(systemName: icon)
            Text(text)
        }
        .font(.caption2.weight(.semibold))
        .padding(.horizontal, 6)
        .padding(.vertical, 2)
        .foregroundStyle(.secondary)
        .background(theme.cardBackgroundProminent(colorScheme))
        .clipShape(Capsule())
    }
}

#Preview("CardProfile") {
    VStack(spacing: 16) {
        CardProfile(
            symbol: "figure.american.football.circle.fill",
            color: "green",
            name: "Quinn",
            favoriteTeamAbbr: "KC",
            favoriteLeagueId: 2,
            careerUnits: 128,
            isEditable: true,
            onEdit: {}
        )
        CardProfile(
            symbol: "figure.basketball.circle.fill",
            color: "blue",
            name: "Runner Two",
            favoriteTeamAbbr: nil,
            favoriteLeagueId: nil,
            careerUnits: -46,
            showsViewProfileButton: true,
            onViewProfile: {}
        )
        CardProfile(symbol: nil, color: nil, name: "Loading…")
    }
    .padding()
    .environmentObject(AppTheme())
}
