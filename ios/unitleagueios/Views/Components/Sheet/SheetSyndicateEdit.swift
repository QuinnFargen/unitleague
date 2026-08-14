import SwiftUI

private enum CapsulePick: Equatable {
    case preset(Int)
    case custom
}

struct SheetSyndicateEdit: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @Binding var syndicate: Syndicate

    @State private var nameInput: String
    @State private var selectedSymbol: String
    @State private var selectedColor: AccentOption
    @State private var maxRunnerPick: CapsulePick
    @State private var maxRunnerCustom: String
    @State private var startUnitsPick: CapsulePick
    @State private var startUnitsCustom: String
    @State private var isEditingName = false
    @State private var isSaving = false
    @State private var errorMessage: String?
    @State private var showingSymbolPicker = false

    private let presets = [5, 10, 20]

    private var maxRunner: Int? {
        switch maxRunnerPick {
        case .preset(let v): return v
        case .custom: return Int(maxRunnerCustom.trimmingCharacters(in: .whitespaces))
        }
    }

    private var startUnits: Int? {
        switch startUnitsPick {
        case .preset(let v): return v
        case .custom: return Int(startUnitsCustom.trimmingCharacters(in: .whitespaces))
        }
    }

    private var isValid: Bool {
        let nameOK = !nameInput.trimmingCharacters(in: .whitespaces).isEmpty
        let startUnitsOK = syndicate.isStarted || startUnits != nil
        return nameOK && maxRunner != nil && startUnitsOK
    }

    private static func pick(for value: Int?, presets: [Int]) -> (CapsulePick, String) {
        guard let value else { return (.preset(presets[1]), "") }
        if presets.contains(value) { return (.preset(value), "") }
        return (.custom, "\(value)")
    }

    init(syndicate: Binding<Syndicate>) {
        _syndicate = syndicate
        _nameInput = State(initialValue: syndicate.wrappedValue.name)
        _selectedSymbol = State(initialValue: syndicate.wrappedValue.symbol ?? SyndicateOption.symbols[0])
        _selectedColor = State(initialValue: AccentOption(rawValue: syndicate.wrappedValue.color ?? "") ?? .green)

        let presets = [5, 10, 20]
        let (maxRunnerPick, maxRunnerCustom) = Self.pick(for: syndicate.wrappedValue.maxRunner, presets: presets)
        _maxRunnerPick = State(initialValue: maxRunnerPick)
        _maxRunnerCustom = State(initialValue: maxRunnerCustom)
        let (startUnitsPick, startUnitsCustom) = Self.pick(for: syndicate.wrappedValue.startUnits, presets: presets)
        _startUnitsPick = State(initialValue: startUnitsPick)
        _startUnitsCustom = State(initialValue: startUnitsCustom)
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
                                ForEach(AccentOption.primary) { option in
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

                        VStack(spacing: 16) {
                            if syndicate.isStarted {
                                valueRow(title: "Start Units", value: "\(syndicate.startUnits ?? 0)")
                            } else {
                                capsulePickerSection(
                                    label: "Start Units",
                                    pick: $startUnitsPick,
                                    customText: $startUnitsCustom
                                )
                            }

                            capsulePickerSection(
                                label: "Max Members",
                                pick: $maxRunnerPick,
                                customText: $maxRunnerCustom
                            )
                        }
                        .padding(.horizontal, 32)

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
                        .disabled(!isValid || isSaving)
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
                    selectedColor: $selectedColor,
                    title: "Syndicate Symbol"
                )
            }
        }
    }

    @ViewBuilder
    private func capsulePickerSection(label: String, pick: Binding<CapsulePick>, customText: Binding<String>) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(label)
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 4)

            HStack(spacing: 8) {
                ForEach(presets, id: \.self) { value in
                    capsuleChip(title: "\(value)", isSelected: pick.wrappedValue == .preset(value)) {
                        pick.wrappedValue = .preset(value)
                    }
                }
                capsuleChip(title: "#", isSelected: pick.wrappedValue == .custom) {
                    pick.wrappedValue = .custom
                }
            }

            if pick.wrappedValue == .custom {
                TextField("Enter number", text: customText)
                    .keyboardType(.numberPad)
                    .padding(10)
                    .background(theme.cardBackground(colorScheme))
                    .clipShape(RoundedRectangle(cornerRadius: 10))
            }
        }
    }

    @ViewBuilder
    private func capsuleChip(title: String, isSelected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Text(title)
                .font(.subheadline.weight(isSelected ? .semibold : .regular))
                .foregroundStyle(isSelected ? theme.chipSelectedFG(colorScheme) : theme.primaryText(colorScheme))
                .padding(.horizontal, 16)
                .padding(.vertical, 8)
                .background(isSelected ? theme.chipSelected(colorScheme) : theme.chipUnselected(colorScheme))
                .clipShape(Capsule())
        }
    }

    @ViewBuilder
    private func valueRow(title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 4)

            HStack {
                Text(value)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
                Spacer()
            }
            .padding()
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
        }
    }

    private func save() {
        let trimmedName = nameInput.trimmingCharacters(in: .whitespaces)
        guard !trimmedName.isEmpty else { return }
        isSaving = true
        errorMessage = nil
        let id = syndicate.syndicateId
        let startUnitsToSave = syndicate.isStarted ? nil : startUnits
        Task {
            do {
                let updated = try await SyndicateService().updateSyndicate(
                    syndicateId: id,
                    name: trimmedName,
                    symbol: selectedSymbol,
                    color: selectedColor.rawValue,
                    maxRunner: maxRunner,
                    startUnits: startUnitsToSave
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
