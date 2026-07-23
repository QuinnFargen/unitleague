//
//  JoinSyndicateSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in TabSyndicateView


import SwiftUI

private enum JoinMode: String, CaseIterable {
    case publicMode = "Public"
    case privateMode = "Private"
}

struct SheetSyndicateJoin: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    let bettorId: Int

    @State private var mode: JoinMode = .publicMode

    // Private join state
    @State private var syndicateCode = ""
    @State private var password = ""
    @State private var isLoading = false
    @State private var errorMessage: String?

    // Public browse state
    @State private var leagues: [League] = []
    @State private var selectedLeagueId: Int? = nil
    @State private var publicSyndicates: [Syndicate] = []
    @State private var isLoadingPublic = false
    @State private var publicError: String?

    private var codeIsValid: Bool { !syndicateCode.trimmingCharacters(in: .whitespaces).isEmpty }

    private func join() {
        guard codeIsValid else { return }
        isLoading = true
        errorMessage = nil
        Task {
            do {
                _ = try await SyndicateService().joinSyndicate(
                    bettorId: bettorId,
                    code: syndicateCode.trimmingCharacters(in: .whitespaces),
                    password: password.isEmpty ? nil : password
                )
                dismiss()
            } catch {
                errorMessage = error.localizedDescription
            }
            isLoading = false
        }
    }

    private func loadLeagues() async {
        leagues = (try? await LeagueService().fetchLeagues()) ?? []
    }

    private func loadPublicSyndicates() async {
        isLoadingPublic = true
        publicError = nil
        do {
            publicSyndicates = try await SyndicateService().fetchPublicSyndicates(bettorId: bettorId, leagueId: selectedLeagueId)
        } catch {
            publicError = error.localizedDescription
        }
        isLoadingPublic = false
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                VStack(spacing: 0) {
                    modePicker

                    if mode == .publicMode {
                        publicBrowseView
                    } else {
                        privateJoinForm
                    }
                }

                if isLoading {
                    Color.black.opacity(0.2).ignoresSafeArea()
                    ProgressView()
                }
            }
            .navigationTitle("Join Syndicate")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                if mode == .privateMode {
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Join") { join() }
                            .disabled(!codeIsValid || isLoading)
                            .tint(theme.accent)
                    }
                }
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
            .task { await loadLeagues() }
            .task(id: selectedLeagueId) { await loadPublicSyndicates() }
        }
    }

    private var modePicker: some View {
        HStack(spacing: 8) {
            ForEach(JoinMode.allCases, id: \.self) { m in
                FilterChip(label: m.rawValue, isSelected: mode == m) {
                    mode = m
                }
            }
        }
        .padding(.horizontal)
        .padding(.vertical, 12)
    }

    private var privateJoinForm: some View {
        Form {
            Section("Syndicate Code") {
                TextField("Enter syndicate code", text: $syndicateCode)
                    .autocorrectionDisabled()
                    .textInputAutocapitalization(.characters)
            }

            Section("Password (optional)") {
                SecureField("Enter password", text: $password)
            }

            if let error = errorMessage {
                Section {
                    Text(error)
                        .font(.caption)
                        .foregroundStyle(theme.error)
                }
            }
        }
        .scrollContentBackground(.hidden)
    }

    private var publicBrowseView: some View {
        VStack(spacing: 0) {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    FilterChip(label: "All", isSelected: selectedLeagueId == nil) {
                        selectedLeagueId = nil
                    }
                    ForEach(leagues) { league in
                        FilterChip(label: league.abbr, isSelected: selectedLeagueId == league.id) {
                            selectedLeagueId = league.id
                        }
                    }
                }
                .padding(.horizontal)
                .padding(.bottom, 8)
            }

            if isLoadingPublic && publicSyndicates.isEmpty {
                Spacer()
                ProgressView()
                Spacer()
            } else if let error = publicError {
                Spacer()
                Text(error)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
                Spacer()
            } else if publicSyndicates.isEmpty {
                Spacer()
                Text("No public syndicates with open spots right now.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
                Spacer()
            } else {
                ScrollView {
                    LazyVStack(spacing: 8) {
                        ForEach(publicSyndicates) { syn in
                            NavigationLink(destination: ViewSyndicate(syndicate: syn, onJoined: { dismiss() })) {
                                CardSyndicate(syndicate: syn)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal)
                    .padding(.top, 4)
                }
            }
        }
    }
}

#Preview("SheetJoinSyndicate") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetSyndicateJoin(bettorId: 42)
            .environmentObject(AppTheme())
    }
}
