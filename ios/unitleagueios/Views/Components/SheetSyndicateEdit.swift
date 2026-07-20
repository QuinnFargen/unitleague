import SwiftUI

struct SheetSyndicateEdit: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @Binding var syndicate: Syndicate

    @State private var nameInput: String
    @State private var selectedSymbol: String
    @State private var selectedColor: AccentOption
    @State private var isEditingName = false
    @State private var isSaving = false
    @State private var errorMessage: String?
    @State private var showingSymbolPicker = false

    init(syndicate: Binding<Syndicate>) {
        _syndicate = syndicate
        _nameInput = State(initialValue: syndicate.wrappedValue.name)
        _selectedSymbol = State(initialValue: syndicate.wrappedValue.symbol ?? SyndicateOption.symbols[0])
        _selectedColor = State(initialValue: AccentOption(rawValue: syndicate.wrappedValue.color ?? "") ?? .green)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 32) {
                        Button {
                            showingSymbolPicker = true
                        } label: {
                            ZStack(alignment: .bottomTrailing) {
                                Image(systemName: selectedSymbol)
                                    .font(.system(size: 72))
                                    .foregroundStyle(selectedColor.color)
                                    .frame(width: 108, height: 108)

                                Image(systemName: "pencil.circle.fill")
                                    .font(.title2)
                                    .foregroundStyle(selectedColor.color)
                                    .background(Circle().fill(theme.appBackground(colorScheme)))
                                    .clipShape(Circle())
                            }
                        }
                        .buttonStyle(.plain)
                        .padding(.top, 32)

                        if isEditingName {
                            HStack(spacing: 12) {
                                TextField("Syndicate name", text: $nameInput)
                                    .font(.title2)
                                    .fontWeight(.semibold)
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .multilineTextAlignment(.center)
                                    .autocorrectionDisabled()

                                Button("Save") {
                                    isEditingName = false
                                }
                                .tint(theme.accent)
                            }
                            .padding(.horizontal, 40)
                        } else {
                            Button {
                                isEditingName = true
                            } label: {
                                HStack(spacing: 8) {
                                    Text(nameInput)
                                        .font(.title)
                                        .fontWeight(.semibold)
                                        .foregroundStyle(theme.primaryText(colorScheme))
                                    Image(systemName: "pencil")
                                        .font(.body)
                                        .foregroundStyle(.secondary)
                                }
                            }
                            .buttonStyle(.plain)
                        }

                        if let code = syndicate.code {
                            Text("Join Syndicate Code: \(code)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .padding(.top, 4)
                        }

                        VStack(alignment: .leading, spacing: 10) {
                            Text("Syndicate Color")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 32)

                            HStack(spacing: 12) {
                                ForEach(AccentOption.allCases) { option in
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
                            .padding(.horizontal, 24)
                        }

                        if let error = errorMessage {
                            Text(error)
                                .font(.caption)
                                .foregroundStyle(theme.error)
                                .multilineTextAlignment(.center)
                                .padding(.horizontal, 32)
                        }

                        Button {
                            save()
                        } label: {
                            Text("Save Syndicate")
                                .font(.body).fontWeight(.medium)
                                .foregroundStyle(theme.accent)
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 14)
                                .background(theme.accent.opacity(0.12))
                                .clipShape(RoundedRectangle(cornerRadius: 12))
                        }
                        .disabled(nameInput.trimmingCharacters(in: .whitespaces).isEmpty || isSaving)
                        .padding(.horizontal, 32)
                        .padding(.bottom, 40)
                    }
                }
            }
            .navigationTitle("Edit Syndicate")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
            .sheet(isPresented: $showingSymbolPicker) {
                SheetSymbolPicker(
                    selectedSymbol: $selectedSymbol,
                    symbols: SyndicateOption.symbols,
                    accentColor: selectedColor.color,
                    title: "Syndicate Symbol"
                )
            }
        }
    }

    private func save() {
        let trimmedName = nameInput.trimmingCharacters(in: .whitespaces)
        guard !trimmedName.isEmpty else { return }
        isSaving = true
        errorMessage = nil
        let id = syndicate.syndicateId
        Task {
            do {
                let updated = try await SyndicateService().updateSyndicate(
                    syndicateId: id,
                    name: trimmedName,
                    symbol: selectedSymbol,
                    color: selectedColor.rawValue
                )
                syndicate = updated
                dismiss()
            } catch {
                errorMessage = error.localizedDescription
            }
            isSaving = false
        }
    }
}

#Preview("SheetSyndicateEdit") {
    SheetSyndicateEdit(syndicate: .constant(Mock.syndicate))
        .environmentObject(AppTheme())
}
