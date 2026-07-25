import SwiftUI

struct CardBookmarkParlay: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let legs: [PlacedBet]
    var onTap: () -> Void
    var onRemove: () -> Void

    @State private var showRemoveConfirm = false

    private var combinedOdds: Double { legs.map(\.price).reduce(1.0, *) }

    var body: some View {
        Button(action: onTap) {
            VStack(alignment: .leading, spacing: 10) {
                HStack {
                    Text("Parlay · \(legs.count) legs")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Spacer()
                    HStack(alignment: .firstTextBaseline, spacing: 2) {
                        Text(String(format: "%.2f", combinedOdds))
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.accent)
                        Text("x")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(theme.accent)
                    }
                }

                Divider()

                ForEach(legs) { leg in
                    CardBet(bet: SelectedBet(placedBet: leg))
                }
            }
            .padding(14)
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
            .overlay(RoundedRectangle(cornerRadius: 14).strokeBorder(theme.divider(colorScheme), lineWidth: 0.5))
        }
        .buttonStyle(.plain)
        .onLongPressGesture { showRemoveConfirm = true }
        .confirmationDialog("Remove this parlay bookmark?", isPresented: $showRemoveConfirm, titleVisibility: .visible) {
            Button("Remove Bookmark", role: .destructive) { onRemove() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will remove all legs of this bookmarked parlay.")
        }
    }
}

#Preview("CardBookmarkParlay") {
    CardBookmarkParlay(legs: [Mock.placedBetML, Mock.placedBetSPR], onTap: {}, onRemove: {})
        .padding()
        .environmentObject(AppTheme())
}
