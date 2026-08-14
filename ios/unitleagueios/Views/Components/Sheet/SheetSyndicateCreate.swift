//
//  CreateSyndicateSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in TabSyndicateView

import SwiftUI

struct SheetSyndicateCreate: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    let bettorId: Int

    @State private var name = ""
    @State private var description = ""
    @State private var password = ""
    @State private var isPublic = false
    @State private var selectedSymbol: String = SyndicateOption.symbols[0]
    @State private var selectedColor: AccentOption = .green
    @State private var selectedLeagueIds: Set<Int> = []
    @State private var syndicateType: String?
    @State private var leagues: [League] = []
    @State private var isEditingName = false
    @State private var isLoading = false
    @State private var isLoadingLeagues = false
    @State private var errorMessage: String?
    @State private var showingSymbolPicker = false

    private var isValid: Bool {
        !name.trimmingCharacters(in: .whitespaces).isEmpty
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
                    symbol: selectedSymbol,
                    color: selectedColor.rawValue,
                    syndicateType: syndicateType,
                    leagueIds: selectedLeagueIds.isEmpty ? nil : Array(selectedLeagueIds).sorted()
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

                        VStack(spacing: 16) {
                            VStack(alignment: .leading, spacing: 8) {
                                Text("Leagues")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .padding(.horizontal, 4)

                                if isLoadingLeagues && leagues.isEmpty {
                                    ProgressView().frame(maxWidth: .infinity)
                                } else {
                                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 80), spacing: 8)], spacing: 8) {
                                        ForEach(leagues) { league in
                                            capsuleChip(title: league.abbr, isSelected: selectedLeagueIds.contains(league.id)) {
                                                if selectedLeagueIds.contains(league.id) {
                                                    selectedLeagueIds.remove(league.id)
                                                } else {
                                                    selectedLeagueIds.insert(league.id)
                                                }
                                            }
                                        }
                                    }
                                    Text("No leagues selected allows every league.")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Syndicate Type")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .padding(.horizontal, 4)

                                HStack(spacing: 8) {
                                    ForEach(SyndicateType.allCases, id: \.self) { type in
                                        capsuleChip(title: type.rawValue, isSelected: syndicateType == type.rawValue) {
                                            syndicateType = (syndicateType == type.rawValue) ? nil : type.rawValue
                                        }
                                    }
                                }
                            }

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
            .sheet(isPresented: $showingSymbolPicker) {
                SheetSymbolPicker(
                    selectedSymbol: $selectedSymbol,
                    symbols: SyndicateOption.symbols,
                    selectedColor: $selectedColor,
                    title: "Syndicate Symbol"
                )
            }
            .task {
                isLoadingLeagues = true
                leagues = (try? await LeagueService().fetchLeagues()) ?? []
                isLoadingLeagues = false
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
