//
//  CreateSyndicateSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in TabSyndicateView

import SwiftUI

private enum CapsulePick: Equatable {
    case preset(Int)
    case custom
}

struct SheetSyndicateCreate: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    let bettorId: Int

    @State private var name = ""
    @State private var description = ""
    @State private var password = ""
    @State private var maxRunnerPick: CapsulePick = .preset(10)
    @State private var maxRunnerCustom = ""
    @State private var startUnitsPick: CapsulePick = .preset(10)
    @State private var startUnitsCustom = ""
    @State private var isPublic = false
    @State private var selectedSymbol: String = SyndicateOption.symbols[0]
    @State private var selectedColor: AccentOption = .green
    @State private var isEditingName = false
    @State private var isLoading = false
    @State private var errorMessage: String?

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
        !name.trimmingCharacters(in: .whitespaces).isEmpty && maxRunner != nil && startUnits != nil
    }

    private func create() {
        isLoading = true
        errorMessage = nil
        Task {
            do {
                _ = try await SyndicateService().createSyndicate(
                    bettorId: bettorId,
                    name: name.trimmingCharacters(in: .whitespaces),
                    description: description.trimmingCharacters(in: .whitespaces).isEmpty ? nil : description,
                    isPublic: isPublic,
                    password: password.isEmpty ? nil : password,
                    maxRunner: maxRunner,
                    startUnits: startUnits,
                    symbol: selectedSymbol,
                    color: selectedColor.rawValue
                )
                dismiss()
            } catch {
                errorMessage = error.localizedDescription
            }
            isLoading = false
        }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 32) {
                        Image(systemName: selectedSymbol)
                            .font(.system(size: 72))
                            .foregroundStyle(selectedColor.color)
                            .padding(.top, 32)

                        if isEditingName {
                            HStack(spacing: 12) {
                                TextField("Syndicate name", text: $name)
                                    .font(.title2)
                                    .fontWeight(.semibold)
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .multilineTextAlignment(.center)
                                    .autocorrectionDisabled()

                                Button("Save") { isEditingName = false }
                                    .tint(theme.accent)
                            }
                            .padding(.horizontal, 40)
                        } else {
                            Button {
                                isEditingName = true
                            } label: {
                                HStack(spacing: 8) {
                                    Text(name.isEmpty ? "Syndicate Name" : name)
                                        .font(.title)
                                        .fontWeight(.semibold)
                                        .foregroundStyle(name.isEmpty ? Color.secondary : theme.primaryText(colorScheme))
                                    Image(systemName: "pencil")
                                        .font(.body)
                                        .foregroundStyle(.secondary)
                                }
                            }
                            .buttonStyle(.plain)
                        }

                        VStack(alignment: .leading, spacing: 10) {
                            Text("Symbol")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 32)

                            HStack(spacing: 14) {
                                ForEach(SyndicateOption.symbols, id: \.self) { symbol in
                                    Button {
                                        selectedSymbol = symbol
                                    } label: {
                                        Image(systemName: symbol)
                                            .font(.title2)
                                            .foregroundStyle(selectedSymbol == symbol ? theme.primaryText(colorScheme) : .secondary)
                                            .frame(width: 52, height: 52)
                                            .background(selectedSymbol == symbol ? theme.cardBackgroundProminent(colorScheme) : Color.clear)
                                            .clipShape(RoundedRectangle(cornerRadius: 12))
                                            .overlay(
                                                RoundedRectangle(cornerRadius: 12)
                                                    .stroke(
                                                        selectedSymbol == symbol ? theme.primaryText(colorScheme).opacity(0.4) : Color.clear,
                                                        lineWidth: 1.5
                                                    )
                                            )
                                    }
                                }
                            }
                            .padding(.horizontal, 20)
                        }

                        VStack(alignment: .leading, spacing: 10) {
                            Text("Color")
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
                                                .shadow(color: option.color.opacity(selectedColor == option ? 0.6 : 0), radius: 6)
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
                            capsulePickerSection(
                                label: "Start Units",
                                pick: $startUnitsPick,
                                customText: $startUnitsCustom
                            )

                            capsulePickerSection(
                                label: "Max Members",
                                pick: $maxRunnerPick,
                                customText: $maxRunnerCustom
                            )

                            Toggle("Public", isOn: $isPublic)
                                .tint(theme.accent)
                                .padding(.horizontal, 4)

                            VStack(alignment: .leading, spacing: 6) {
                                Text("Description (optional)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .padding(.horizontal, 4)
                                TextField("Add a description", text: $description)
                                    .padding(12)
                                    .background(theme.cardBackground(colorScheme))
                                    .clipShape(RoundedRectangle(cornerRadius: 10))
                            }

                            VStack(alignment: .leading, spacing: 6) {
                                Text("Password (optional)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .padding(.horizontal, 4)
                                SecureField("Set a password", text: $password)
                                    .padding(12)
                                    .background(theme.cardBackground(colorScheme))
                                    .clipShape(RoundedRectangle(cornerRadius: 10))
                            }
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
                            create()
                        } label: {
                            Text("Create")
                                .font(.body).fontWeight(.medium)
                                .foregroundStyle(theme.accent)
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 14)
                                .background(theme.accent.opacity(0.12))
                                .clipShape(RoundedRectangle(cornerRadius: 12))
                        }
                        .disabled(!isValid || isLoading)
                        .padding(.horizontal, 32)
                        .padding(.bottom, 40)
                    }
                }

                if isLoading {
                    Color.black.opacity(0.2).ignoresSafeArea()
                    ProgressView()
                }
            }
            .navigationTitle("Create Syndicate")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
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
}

#Preview("SheetCreateSyndicate") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSyndicateCreate(bettorId: 42)
            .environmentObject(AppTheme())
    }
}
