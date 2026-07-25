import SwiftUI

struct CardBookmarkSingle: View {
    let bookmark: PlacedBet
    var onTap: () -> Void
    var onRemove: () -> Void

    @State private var showRemoveConfirm = false

    var body: some View {
        Button(action: onTap) {
            CardBet(bet: SelectedBet(placedBet: bookmark))
        }
        .buttonStyle(.plain)
        .onLongPressGesture { showRemoveConfirm = true }
        .confirmationDialog("Remove this bookmark?", isPresented: $showRemoveConfirm, titleVisibility: .visible) {
            Button("Remove Bookmark", role: .destructive) { onRemove() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will remove the bookmarked bet.")
        }
    }
}

#Preview("CardBookmarkSingle") {
    CardBookmarkSingle(bookmark: Mock.placedBetML, onTap: {}, onRemove: {})
        .padding()
        .environmentObject(AppTheme())
}
