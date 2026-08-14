//
//  EditProfileSheet.swift
//  unitleagueios
//
//  Created by Quinn Fargen on 5/23/26.
//
//  Used in TabProfileView. Also reused (via the `runner` init param) by
//  ViewSyndicate to edit a Runner's name/symbol/color within a syndicate.


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
    @AppStorage("notificationsEnabled") private var notificationsEnabled: Bool = true

    /// When set, the sheet edits this syndicate Runner's name/symbol/color instead of the
    /// signed-in bettor's own profile (email + notifications are hidden in this mode).
    var runner: Binding<Runner>?

    @State private var isEditingUsername = false
    @State private var usernameInput = ""
    @State private var isEditingEmail = false
    @State private var emailInput = ""
    @State private var showingSymbolPicker = false
    @State private var isSaving = false
    @State private var errorMessage: String?

    @State private var runnerName: String
    @State private var runnerSymbol: String
    @State private var runnerColor: AccentOption

    private var isRunnerMode: Bool { runner != nil }

    private var displayName: String {
        customUserName.isEmpty ? appleUserName : customUserName
    }

    private var symbolBinding: Binding<String> {
        isRunnerMode ? $runnerSymbol : $profileSymbol
    }

    private var displayColor: Color {
        isRunnerMode ? runnerColor.color : theme.accent
    }

    private var colorBinding: Binding<AccentOption> {
        if isRunnerMode {
            return $runnerColor
        }
        return Binding(get: { theme.accentOption }, set: { theme.accentOption = $0 })
    }

    /// Green/red are reserved for win/loss indicators, so they're excluded from the
    /// signed-in bettor's own profile color — runner colors aren't restricted.
    private var blockedColors: Set<AccentOption> {
        isRunnerMode ? [] : AccentOption.blockedForProfile
    }

    private var availableColorOptions: [AccentOption] {
        AccentOption.primary.filter { !blockedColors.contains($0) }
    }

    init(runner: Binding<Runner>? = nil) {
        self.runner = runner
        _runnerName = State(initialValue: runner?.wrappedValue.profileName ?? "")
        _runnerSymbol = State(initialValue: runner?.wrappedValue.symbol ?? ProfileOption.symbols[0])
        _runnerColor = State(initialValue: AccentOption(rawValue: runner?.wrappedValue.color ?? "") ?? .green)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 32) {
                        symbolHeader

                        nameSection

                        if !isRunnerMode {
                            emailSection
                        }

                        colorSection(
                            title: isRunnerMode ? "Runner Color" : "Accent Color",
                            options: availableColorOptions,
                            selection: colorBinding
                        )

                        if !isRunnerMode {
                            notificationsSection
                        }

                        if let error = errorMessage {
                            Text(error)
                                .font(.caption)
                                .foregroundStyle(theme.error)
                                .multilineTextAlignment(.center)
                                .padding(.horizontal, 32)
                        }

                        VStack(spacing: 12) {
                            Button {
                                save()
                            } label: {
                                HStack(spacing: 8) {
                                    if isSaving { ProgressView().scaleEffect(0.8) }
                                    Text(isRunnerMode ? "Save Runner" : "Save Profile")
                                        .font(.body).fontWeight(.medium)
                                }
                                .foregroundStyle(theme.accent)
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 14)
                                .background(theme.accent.opacity(0.12))
                                .clipShape(RoundedRectangle(cornerRadius: 12))
                            }
                            .disabled(isSaving || (isRunnerMode && runnerName.trimmingCharacters(in: .whitespaces).isEmpty))

                            if !isRunnerMode {
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
                        }
                        .padding(.horizontal, 32)
                        .padding(.top, 8)
                        .padding(.bottom, 40)
                    }
                }
            }
            .navigationTitle(isRunnerMode ? "Edit Runner" : "Edit Profile")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") {
                        if isRunnerMode || profileSaved { dismiss() }
                    }
                    .disabled(!isRunnerMode && !profileSaved)
                }
            }
            .sheet(isPresented: $showingSymbolPicker) {
                SheetSymbolPicker(
                    selectedSymbol: symbolBinding,
                    symbols: ProfileOption.symbols,
                    selectedColor: colorBinding,
                    blockedColors: blockedColors,
                    title: isRunnerMode ? "Runner Symbol" : "Profile Symbol"
                )
            }
        }
    }

    private var symbolHeader: some View {
        Button {
            showingSymbolPicker = true
        } label: {
            ZStack(alignment: .bottomTrailing) {
                Image(systemName: symbolBinding.wrappedValue)
                    .font(.system(size: 72))
                    .foregroundStyle(displayColor)
                    .frame(width: 108, height: 108)

                Image(systemName: "pencil.circle.fill")
                    .font(.title2)
                    .foregroundStyle(displayColor)
                    .background(Circle().fill(theme.appBackground(colorScheme)))
                    .clipShape(Circle())
            }
        }
        .buttonStyle(.plain)
        .padding(.top, 32)
    }

    @ViewBuilder
    private var nameSection: some View {
        if isEditingUsername {
            HStack(spacing: 12) {
                TextField(isRunnerMode ? "Runner name" : "Username", text: $usernameInput)
                    .font(.title2)
                    .fontWeight(.semibold)
                    .foregroundStyle(theme.primaryText(colorScheme))
                    .multilineTextAlignment(.center)
                    .autocorrectionDisabled()
                    .textInputAutocapitalization(.never)

                Button("Save") {
                    let trimmed = usernameInput.trimmingCharacters(in: .whitespaces)
                    if !trimmed.isEmpty {
                        if isRunnerMode { runnerName = trimmed } else { customUserName = trimmed }
                    }
                    isEditingUsername = false
                }
                .tint(theme.accent)
            }
            .padding(.horizontal, 40)
        } else {
            Button {
                usernameInput = isRunnerMode ? runnerName : displayName
                isEditingUsername = true
            } label: {
                HStack(spacing: 8) {
                    Text(isRunnerMode ? (runnerName.isEmpty ? "Runner Name" : runnerName) : displayName)
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
    }

    @ViewBuilder
    private var emailSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Email")
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 32)

            if isEditingEmail {
                HStack(spacing: 12) {
                    TextField("Email", text: $emailInput)
                        .font(.body)
                        .foregroundStyle(theme.primaryText(colorScheme))
                        .keyboardType(.emailAddress)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()

                    Button("Save") {
                        let trimmed = emailInput.trimmingCharacters(in: .whitespaces)
                        if !trimmed.isEmpty { appleEmail = trimmed }
                        isEditingEmail = false
                    }
                    .tint(theme.accent)
                }
                .padding(.horizontal, 32)
            } else {
                Button {
                    emailInput = appleEmail
                    isEditingEmail = true
                } label: {
                    HStack(spacing: 8) {
                        Text(appleEmail.isEmpty ? "Add email" : appleEmail)
                            .font(.body)
                            .foregroundStyle(appleEmail.isEmpty ? Color.secondary : theme.primaryText(colorScheme))
                        Image(systemName: "pencil")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .padding(.horizontal, 32)
                }
                .buttonStyle(.plain)
            }
        }
    }

    private var notificationsSection: some View {
        Toggle(isOn: $notificationsEnabled) {
            Text("Notifications")
                .foregroundStyle(theme.primaryText(colorScheme))
        }
        .tint(theme.accent)
        .padding(.horizontal, 32)
    }

    @ViewBuilder
    private func colorSection(title: String, options: [AccentOption], selection: Binding<AccentOption>) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 32)

            HStack(spacing: 12) {
                ForEach(options) { option in
                    Button {
                        selection.wrappedValue = option
                    } label: {
                        VStack(spacing: 4) {
                            Circle()
                                .fill(option.color)
                                .frame(width: 36, height: 36)
                                .overlay(
                                    Circle()
                                        .stroke(theme.primaryText(colorScheme), lineWidth: selection.wrappedValue == option ? 2.5 : 0)
                                )
                                .shadow(
                                    color: option.color.opacity(selection.wrappedValue == option ? 0.6 : 0),
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
    }

    private func save() {
        if isEditingUsername {
            let trimmed = usernameInput.trimmingCharacters(in: .whitespaces)
            if !trimmed.isEmpty {
                if isRunnerMode { runnerName = trimmed } else { customUserName = trimmed }
            }
            isEditingUsername = false
        }
        if isEditingEmail, !isRunnerMode {
            let trimmed = emailInput.trimmingCharacters(in: .whitespaces)
            if !trimmed.isEmpty { appleEmail = trimmed }
            isEditingEmail = false
        }

        if let runner {
            let runnerId = runner.wrappedValue.runnerId
            let name = runnerName.trimmingCharacters(in: .whitespaces)
            let symbol = runnerSymbol
            let color = runnerColor.rawValue
            guard !name.isEmpty else { return }
            isSaving = true
            errorMessage = nil
            Task {
                do {
                    let updated = try await RunnerService().updateRunner(
                        runnerId: runnerId,
                        profileName: name,
                        symbol: symbol,
                        color: color
                    )
                    runner.wrappedValue = updated
                    dismiss()
                } catch {
                    errorMessage = error.localizedDescription
                }
                isSaving = false
            }
        } else {
            profileSaved = true
            let id = bettorId
            let name = customUserName.isEmpty ? appleUserName : customUserName
            let symbol = profileSymbol
            let color = theme.accentOption.rawValue
            let email = appleEmail
            if id != 0 {
                Task {
                    try? await BettorService().updateProfile(
                        bettorId: id,
                        profileName: name,
                        symbol: symbol,
                        color: color,
                        appleEmail: email.isEmpty ? nil : email
                    )
                }
            }
            dismiss()
        }
    }
}

#Preview("SheetEditProfile") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetEditProfile()
            .environmentObject(AppTheme())
    }
}

#Preview("SheetEditProfile - Runner") {
    Color.clear.sheet(isPresented: .constant(true)) {
        SheetEditProfile(runner: .constant(Mock.runnerAdmin))
            .environmentObject(AppTheme())
    }
}
