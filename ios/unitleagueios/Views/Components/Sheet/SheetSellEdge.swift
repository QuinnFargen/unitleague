import SwiftUI

struct SheetSellEdge: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    let edges: [EnhancedRow]
    let onSelect: (EnhancedRow) -> Void

    var body: some View {
        NavigationStack {
            List(edges) { edge in
                Button {
                    onSelect(edge)
                    dismiss()
                } label: {
                    HStack(spacing: 12) {
                        Image(systemName: edge.symbol ?? "bolt.fill")
                            .foregroundStyle(theme.accent)
                        Text(edge.name)
                            .foregroundStyle(theme.primaryText(colorScheme))
                        Spacer()
                    }
                }
            }
            .navigationTitle("Sell an Edge")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
        }
    }
}

#Preview("SheetSellEdge") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSellEdge(edges: [], onSelect: { _ in })
            .environmentObject(AppTheme())
    }
}
