import SwiftUI

struct SheetBrowseEnhancements: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    @State private var definitions: [EnhancementDef] = []
    @State private var isLoading = false

    private let enhancementService = EnhancementService()

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()
                if isLoading {
                    ProgressView()
                } else {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 16) {
                            let clv = definitions.filter { $0.enhancementType == "clv" }
                            let team = definitions.filter { $0.enhancementType == "team" }
                            let edge = definitions.filter { $0.enhancementType == "edge" }

                            if !clv.isEmpty {
                                EnhancementGroupHeader("CLV", color: .green)
                                ForEach(clv) { CardEnhancementDef(def: $0) }
                            }
                            if !team.isEmpty {
                                EnhancementGroupHeader("Team", color: .yellow)
                                ForEach(team) { CardEnhancementDef(def: $0) }
                            }
                            if !edge.isEmpty {
                                EnhancementGroupHeader("Edge", color: .red)
                                ForEach(edge) { CardEnhancementDef(def: $0) }
                            }
                        }
                        .padding(16)
                    }
                }
            }
            .navigationTitle("All Enhancements")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .task { await load() }
        }
    }

    private func load() async {
        isLoading = true
        defer { isLoading = false }
        definitions = (try? await enhancementService.fetchEnhancements()) ?? []
    }
}

private struct CardEnhancementDef: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let def: EnhancementDef

    var body: some View {
        HStack(spacing: 12) {
            EnhancementSymbolIcon(type: def.enhancementType, symbol: def.symbol)
            VStack(alignment: .leading, spacing: 4) {
                Text(def.name)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                if let description = def.description {
                    Text(description)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(2)
                }
                if def.enhancementType == "edge" {
                    HStack(spacing: 6) {
                        Text(def.edgeType.capitalized)
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .padding(.horizontal, 6).padding(.vertical, 3)
                            .background(Color.secondary.opacity(0.12))
                            .clipShape(Capsule())
                        if let rarity = def.rarity {
                            Text(rarity.capitalized)
                                .font(.caption2.weight(.bold))
                                .foregroundStyle(enhancementRarityColor(rarity))
                                .padding(.horizontal, 6).padding(.vertical, 3)
                                .background(enhancementRarityColor(rarity).opacity(0.15))
                                .clipShape(Capsule())
                        }
                    }
                    .padding(.top, 2)
                }
            }
            Spacer()
        }
        .padding(12)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }
}

#Preview("SheetBrowseEnhancements") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetBrowseEnhancements()
            .environmentObject(AppTheme())
    }
}
