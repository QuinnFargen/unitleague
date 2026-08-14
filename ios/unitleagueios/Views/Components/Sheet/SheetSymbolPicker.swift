import SwiftUI

struct SheetSymbolPicker: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    @Binding var selectedSymbol: String
    let symbols: [String]
    @Binding var selectedColor: AccentOption
    var blockedColors: Set<AccentOption> = []
    var title: String = "Choose Symbol"

    private let columns = [GridItem(.adaptive(minimum: 64), spacing: 14)]

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(alignment: .leading, spacing: 18) {
                        colorSection

                        LazyVGrid(columns: columns, spacing: 14) {
                            ForEach(symbols, id: \.self) { symbol in
                                Button {
                                    selectedSymbol = symbol
                                    dismiss()
                                } label: {
                                    Image(systemName: symbol)
                                        .font(.title2)
                                        .foregroundStyle(selectedColor.color)
                                        .frame(width: 60, height: 60)
                                        .background(
                                            selectedSymbol == symbol
                                                ? selectedColor.color.opacity(0.18)
                                                : theme.cardBackground(colorScheme)
                                        )
                                        .clipShape(RoundedRectangle(cornerRadius: 14))
                                        .overlay(
                                            RoundedRectangle(cornerRadius: 14)
                                                .stroke(
                                                    selectedSymbol == symbol ? selectedColor.color : Color.clear,
                                                    lineWidth: 2
                                                )
                                        )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                    }
                    .padding(16)
                }
            }
            .navigationTitle(title)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }

    private var colorSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Color")
                .font(.caption)
                .foregroundStyle(.secondary)

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 12) {
                    ForEach(AccentOption.allCases.filter { !blockedColors.contains($0) }) { option in
                        Button {
                            selectedColor = option
                        } label: {
                            VStack(spacing: 4) {
                                Circle()
                                    .fill(option.color)
                                    .frame(width: 36, height: 36)
                                    .overlay(
                                        Circle()
                                            .stroke(theme.primaryText(colorScheme), lineWidth: selectedColor == option ? 2.5 : 0)
                                    )
                                    .shadow(
                                        color: option.color.opacity(selectedColor == option ? 0.6 : 0),
                                        radius: 6
                                    )
                                Text(option.label)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
                .padding(.vertical, 2)
            }
        }
    }
}

#Preview("SheetSymbolPicker") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSymbolPicker(
            selectedSymbol: .constant(ProfileOption.symbols[0]),
            symbols: ProfileOption.symbols,
            selectedColor: .constant(.green),
            title: "Profile Symbol"
        )
        .environmentObject(AppTheme())
    }
}
