//
//  JoinSyndicateSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in TabSyndicateView


import SwiftUI

struct SheetSyndicateJoin: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    let bettorId: Int

    @State private var syndicateCode = ""
    @State private var password = ""
    @State private var isLoading = false
    @State private var errorMessage: String?

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

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

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

                if isLoading {
                    Color.black.opacity(0.2).ignoresSafeArea()
                    ProgressView()
                }
            }
            .navigationTitle("Join Syndicate")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Join") { join() }
                        .disabled(!codeIsValid || isLoading)
                        .tint(theme.accent)
                }
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
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
