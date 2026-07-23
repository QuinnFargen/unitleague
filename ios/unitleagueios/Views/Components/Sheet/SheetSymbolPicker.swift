import SwiftUI

struct SheetSymbolPicker: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    @Binding var selectedSymbol: String
    let symbols: [String]
    let accentColor: Color
    var title: String = "Choose Symbol"

    private let columns = [GridItem(.adaptive(minimum: 64), spacing: 14)]

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    LazyVGrid(columns: columns, spacing: 14) {
                        ForEach(symbols, id: \.self) { symbol in
                            Button {
                                selectedSymbol = symbol
                                dismiss()
                            } label: {
                                Image(systemName: symbol)
                                    .font(.title2)
                                    .foregroundStyle(accentColor)
                                    .frame(width: 60, height: 60)
                                    .background(
                                        selectedSymbol == symbol
                                            ? accentColor.opacity(0.18)
                                            : theme.cardBackground(colorScheme)
                                    )
                                    .clipShape(RoundedRectangle(cornerRadius: 14))
                                    .overlay(
                                        RoundedRectangle(cornerRadius: 14)
                                            .stroke(
                                                selectedSymbol == symbol ? accentColor : Color.clear,
                                                lineWidth: 2
                                            )
                                    )
                            }
                            .buttonStyle(.plain)
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
}

#Preview("SheetSymbolPicker") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSymbolPicker(
            selectedSymbol: .constant(ProfileOption.symbols[0]),
            symbols: ProfileOption.symbols,
            accentColor: .green,
            title: "Profile Symbol"
        )
        .environmentObject(AppTheme())
    }
}
