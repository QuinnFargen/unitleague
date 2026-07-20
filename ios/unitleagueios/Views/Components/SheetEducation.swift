import SwiftUI

struct SheetEducation: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss

    private let categories = ["Math", "Parlays", "Terms", "Traps", "Rules"]
    @State private var selected: Set<String> = []

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                VStack(spacing: 20) {
                    VStack(spacing: 10) {
                        ForEach(categories, id: \.self) { category in
                            Button { toggle(category) } label: {
                                HStack {
                                    Text(category)
                                        .foregroundStyle(theme.primaryText(colorScheme))
                                    Spacer()
                                    if selected.contains(category) {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundStyle(theme.accent)
                                    } else {
                                        Image(systemName: "circle")
                                            .foregroundStyle(.secondary)
                                    }
                                }
                                .padding()
                                .background(theme.cardBackground(colorScheme))
                                .clipShape(RoundedRectangle(cornerRadius: 14))
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 16)

                    Spacer()

                    Button {
                        dismiss()
                    } label: {
                        Text("Start")
                            .font(.headline)
                            .foregroundStyle(theme.chipSelectedFG(colorScheme))
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 14)
                            .background(theme.chipSelected(colorScheme))
                            .clipShape(Capsule())
                    }
                    .disabled(selected.isEmpty)
                    .padding(.horizontal, 16)
                    .padding(.bottom, 16)
                }
            }
            .navigationTitle("Education")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
        }
    }

    private func toggle(_ category: String) {
        if selected.contains(category) { selected.remove(category) } else { selected.insert(category) }
    }
}

#Preview("SheetEducation") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetEducation().environmentObject(AppTheme())
    }
}
