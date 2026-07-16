import SwiftUI
import AuthenticationServices

struct TabProfileView: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("appleUserName")       private var appleUserName: String   = ""
    @AppStorage("customUserName")      private var customUserName: String  = ""
    @AppStorage("profileSymbol")       private var profileSymbol: String   = ProfileOption.symbols[0]
    @AppStorage("profileSaved")        private var profileSaved: Bool      = false
    @AppStorage("bettorId")            private var bettorId: Int           = 0
    @AppStorage("selectedSyndicateId") private var syndicateId: Int        = 0
    @AppStorage("appleSub")            private var appleSub: String        = ""
    @AppStorage("appleEmail")          private var appleEmail: String      = ""
    @State private var authError: String?
    @State private var showingEditProfile = false

    private var displayName: String {
        customUserName.isEmpty ? appleUserName : customUserName
    }

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()

                if appleUserName.isEmpty {
                    signInView
                } else {
                    savedProfileView
                }
            }
            .tabToolbar()
            .sheet(isPresented: $showingEditProfile) {
                SheetEditProfile()
            }
            .onAppear {
                if !appleUserName.isEmpty && !profileSaved {
                    showingEditProfile = true
                }
            }
        }
    }

    private var signInView: some View {
        VStack(spacing: 40) {
            Text("Unit League")
                .font(.system(size: 48, weight: .bold, design: .rounded))
                .foregroundStyle(theme.primaryText(colorScheme))

            SignInWithAppleButton(.signIn) { request in
                request.requestedScopes = [.fullName, .email]
            } onCompletion: { result in
                switch result {
                case .success(let auth):
                    guard let credential = auth.credential as? ASAuthorizationAppleIDCredential else { return }
                    // Apple only returns fullName and email on the very first authorization.
                    // Persist them so subsequent sign-ins still have the values.
                    let first = credential.fullName?.givenName ?? ""
                    let last = credential.fullName?.familyName ?? ""
                    let name = [first, last].filter { !$0.isEmpty }.joined(separator: " ")
                    if !name.isEmpty { appleUserName = name }
                    if appleUserName.isEmpty { appleUserName = "Player" }
                    appleSub = credential.user
                    if let email = credential.email { appleEmail = email }
                    let storedEmail = appleEmail.isEmpty ? nil : appleEmail
                    let storedName = appleUserName == "Player" ? nil : appleUserName
                    
                    Task {
                        do {
                            let bettor = try await BettorService().createBettor(
                                appleSub: appleSub,
                                appleEmail: storedEmail,
                                appleName: storedName
                            )
                            bettorId = bettor.bettorId
                            if let pn = bettor.profileName, !pn.isEmpty { customUserName = pn }
                            if let sym = bettor.symbol, !sym.isEmpty    { profileSymbol = sym }
                            if let col = bettor.color,
                               let accent = AccentOption(rawValue: col)  { theme.accentOption = accent }
                            if !customUserName.isEmpty                   { profileSaved = true }
                        } catch {
                            authError = "Account setup failed: \(error.localizedDescription)"
                        }
                    }
                case .failure(let error):
                    let asError = error as? ASAuthorizationError
                    if asError?.code != .canceled {
                        authError = "Sign in failed. Make sure you're signed into an Apple ID in Settings."
                    }
                }
            }
            .signInWithAppleButtonStyle(.white)
            .frame(height: 50)
            .frame(maxWidth: 280)
            .cornerRadius(8)

            if let msg = authError {
                Text(msg)
                    .font(.footnote)
                    .foregroundStyle(theme.error)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal)
            }

            Button("Sign in as Test User") {
                appleUserName = "Test User"
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
    }

    @AppStorage("useLocalAPI") private var useLocalAPI: Bool = false

    private var savedProfileView: some View {
        ScrollView {
            VStack(spacing: 24) {
                HStack(alignment: .center, spacing: 14) {
                    Image(systemName: profileSymbol)
                        .font(.system(size: 28, weight: .semibold))
                        .foregroundStyle(theme.accent)
                        .frame(width: 48, height: 48)
                        .background(theme.cardBackgroundProminent(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 12))

                    Text(displayName)
                        .font(.title2).bold()
                        .foregroundStyle(theme.primaryText(colorScheme))

                    Spacer()

                    Button {
                        showingEditProfile = true
                    } label: {
                        Image(systemName: "pencil.circle")
                            .font(.title3)
                            .foregroundStyle(theme.accent)
                    }
                    .buttonStyle(.plain)
                }
                .padding()
                .background(theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 14))

                HStack {
                    Image(systemName: "network")
                        .foregroundStyle(theme.accent)
                    Text("Use Local API")
                        .foregroundStyle(theme.primaryText(colorScheme))
                    Spacer()
                    Toggle("", isOn: $useLocalAPI)
                        .labelsHidden()
                        .tint(theme.accent)
                }
                .padding()
                .background(theme.cardBackground(colorScheme))
                .clipShape(RoundedRectangle(cornerRadius: 14))

            }
            .padding(.horizontal, 16)
            .padding(.top, 16)
            .padding(.bottom, 32)
        }
    }
}


enum ProfileOption {
    static let symbols = [
        "figure.american.football.circle.fill",
        "figure.basketball.circle.fill",
        "figure.baseball.circle.fill",
        "figure.hockey.circle.fill",
        "figure.pickleball.circle.fill",
        "figure.equestrian.sports.circle.fill"
    ]

    static let colorNames = ["green", "blue", "orange", "purple", "red"]

    static func color(for name: String) -> Color {
        if let accent = AccentOption(rawValue: name) { return accent.color }
        switch name {
        case "green":  return .green
        case "blue":   return .blue
        case "orange": return .orange
        case "purple": return .purple
        case "red":    return .red
        default:       return .green
        }
    }
}

#Preview {
    TabProfileView()
        .environmentObject(AppTheme())
}
