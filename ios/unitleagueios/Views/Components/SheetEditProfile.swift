//
//  EditProfileSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in TabProfileView


import SwiftUI
import AuthenticationServices

struct SheetEditProfile: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.dismiss) private var dismiss
    @AppStorage("appleUserName")  private var appleUserName: String  = ""
    @AppStorage("customUserName") private var customUserName: String = ""
    @AppStorage("profileSymbol")  private var profileSymbol: String  = ProfileOption.symbols[0]
    @AppStorage("profileSaved")   private var profileSaved: Bool     = false
    @AppStorage("bettorId")       private var bettorId: Int          = 0
    @AppStorage("appleSub")       private var appleSub: String       = ""
    @AppStorage("appleEmail")     private var appleEmail: String     = ""
    @State private var isEditingUsername = false
    @State private var usernameInput = ""

    private var displayName: String {
        customUserName.isEmpty ? appleUserName : customUserName
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 32) {
                        Image(systemName: profileSymbol)
                            .font(.system(size: 72))
                            .foregroundStyle(theme.accent)
                            .padding(.top, 32)

                        if isEditingUsername {
                            HStack(spacing: 12) {
                                TextField("Username", text: $usernameInput)
                                    .font(.title2)
                                    .fontWeight(.semibold)
                                    .foregroundStyle(theme.primaryText(colorScheme))
                                    .multilineTextAlignment(.center)
                                    .autocorrectionDisabled()
                                    .textInputAutocapitalization(.never)

                                Button("Save") {
                                    let trimmed = usernameInput.trimmingCharacters(in: .whitespaces)
                                    if !trimmed.isEmpty { customUserName = trimmed }
                                    isEditingUsername = false
                                }
                                .tint(theme.accent)
                            }
                            .padding(.horizontal, 40)
                        } else {
                            Button {
                                usernameInput = displayName
                                isEditingUsername = true
                            } label: {
                                HStack(spacing: 8) {
                                    Text(displayName)
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

                        VStack(alignment: .leading, spacing: 10) {
                            Text("Profile Symbol")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 32)

                            HStack(spacing: 14) {
                                ForEach(ProfileOption.symbols, id: \.self) { symbol in
                                    Button {
                                        profileSymbol = symbol
                                    } label: {
                                        Image(systemName: symbol)
                                            .font(.title2)
                                            .foregroundStyle(profileSymbol == symbol ? theme.primaryText(colorScheme) : .secondary)
                                            .frame(width: 52, height: 52)
                                            .background(profileSymbol == symbol ? theme.cardBackgroundProminent(colorScheme) : Color.clear)
                                            .clipShape(RoundedRectangle(cornerRadius: 12))
                                            .overlay(
                                                RoundedRectangle(cornerRadius: 12)
                                                    .stroke(
                                                        profileSymbol == symbol ? theme.primaryText(colorScheme).opacity(0.4) : Color.clear,
                                                        lineWidth: 1.5
                                                    )
                                            )
                                    }
                                }
                            }
                            .padding(.horizontal, 20)
                        }

                        VStack(alignment: .leading, spacing: 10) {
                            Text("Accent Color")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 32)

                            HStack(spacing: 12) {
                                ForEach(AccentOption.allCases) { option in
                                    Button {
                                        theme.accentOption = option
                                    } label: {
                                        VStack(spacing: 4) {
                                            Circle()
                                                .fill(option.color)
                                                .frame(width: 36, height: 36)
                                                .overlay(
                                                    Circle()
                                                        .stroke(theme.primaryText(colorScheme), lineWidth: theme.accentOption == option ? 2.5 : 0)
                                                )
                                                .shadow(
                                                    color: option.color.opacity(theme.accentOption == option ? 0.6 : 0),
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

                        VStack(spacing: 12) {
                            Button {
                                if isEditingUsername && !usernameInput.trimmingCharacters(in: .whitespaces).isEmpty {
                                    customUserName = usernameInput.trimmingCharacters(in: .whitespaces)
                                }
                                isEditingUsername = false
                                profileSaved = true
                                let id     = bettorId
                                let name   = customUserName.isEmpty ? appleUserName : customUserName
                                let symbol = profileSymbol
                                let color  = theme.accentOption.rawValue
                                if id != 0 {
                                    Task {
                                        try? await BettorService().updateProfile(
                                            bettorId: id,
                                            profileName: name,
                                            symbol: symbol,
                                            color: color
                                        )
                                    }
                                }
                                dismiss()
                            } label: {
                                Text("Save Profile")
                                    .font(.body).fontWeight(.medium)
                                    .foregroundStyle(theme.accent)
                                    .frame(maxWidth: .infinity)
                                    .padding(.vertical, 14)
                                    .background(theme.accent.opacity(0.12))
                                    .clipShape(RoundedRectangle(cornerRadius: 12))
                            }

                            Button {
                                appleUserName = ""
                                customUserName = ""
                                appleEmail = ""
                                appleSub = ""
                                bettorId = 0
                                profileSaved = false
                                dismiss()
                            } label: {
                                Text("Sign Out")
                                    .font(.body)
                                    .fontWeight(.medium)
                                    .foregroundStyle(theme.error)
                                    .frame(maxWidth: .infinity)
                                    .padding(.vertical, 14)
                                    .background(theme.error.opacity(0.12))
                                    .clipShape(RoundedRectangle(cornerRadius: 12))
                            }
                        }
                        .padding(.horizontal, 32)
                        .padding(.top, 8)
                        .padding(.bottom, 40)
                    }
                }
            }
            .navigationTitle("Edit Profile")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") {
                        if profileSaved { dismiss() }
                    }
                    .disabled(!profileSaved)
                }
            }
        }
    }
}

#Preview("SheetEditProfile") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetEditProfile()
            .environmentObject(AppTheme())
    }
}
