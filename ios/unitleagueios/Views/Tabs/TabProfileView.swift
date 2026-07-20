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
    @State private var showingEducation = false

    @State private var completedRecords: [Txn] = []
    @State private var historySyndicates: [Int: Syndicate] = [:]
    @State private var isLoadingHistory = false

    private let txnService = TxnService()
    private let syndicateService = SyndicateService()

    private var displayName: String {
        customUserName.isEmpty ? appleUserName : customUserName
    }

    private var historyGroups: [(syndicateId: Int, singles: [Txn], parlays: [[Txn]])] {
        let bySyndicate = Dictionary(grouping: completedRecords, by: \.syndicateId)
        return bySyndicate.keys.sorted().map { sid in
            let group = bySyndicate[sid] ?? []
            let singles = group.filter { $0.parlayId == nil }
            let parlayMap = Dictionary(grouping: group.filter { $0.parlayId != nil }, by: { $0.parlayId! })
            let parlays = parlayMap.values.map { $0 }
            return (syndicateId: sid, singles: singles, parlays: parlays)
        }
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
            .sheet(isPresented: $showingEducation) {
                SheetEducation()
            }
            .onAppear {
                if !appleUserName.isEmpty && !profileSaved {
                    showingEditProfile = true
                }
            }
            .task { await loadHistory() }
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

                VStack(alignment: .leading, spacing: 10) {
                    Text("Components")
                        .font(.title3.weight(.bold))
                        .foregroundStyle(theme.primaryText(colorScheme))

                    Button {
                        showingEducation = true
                    } label: {
                        HStack {
                            Image(systemName: "graduationcap.fill")
                                .foregroundStyle(theme.accent)
                            Text("Education")
                                .foregroundStyle(theme.primaryText(colorScheme))
                            Spacer()
                            Image(systemName: "chevron.right")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        .padding()
                        .background(theme.cardBackground(colorScheme))
                        .clipShape(RoundedRectangle(cornerRadius: 14))
                    }
                    .buttonStyle(.plain)
                }

                VStack(alignment: .leading, spacing: 10) {
                    HStack {
                        Text("Bet History")
                            .font(.title3.weight(.bold))
                            .foregroundStyle(theme.primaryText(colorScheme))
                        Spacer()
                        Text("\(completedRecords.count)")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.secondary)
                    }

                    if isLoadingHistory {
                        ProgressView().frame(maxWidth: .infinity).padding(.top, 20)
                    } else if completedRecords.isEmpty {
                        Text("No bet history")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                            .padding(.top, 8)
                    } else {
                        ForEach(historyGroups, id: \.syndicateId) { group in
                            VStack(alignment: .leading, spacing: 10) {
                                let syndicate = historySyndicates[group.syndicateId]
                                HStack(spacing: 6) {
                                    Image(systemName: syndicate?.symbol ?? "house.fill")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(ProfileOption.color(for: syndicate?.color ?? ""))
                                    Text(syndicate?.name ?? "Syndicate \(group.syndicateId)")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(.secondary)
                                }

                                ForEach(group.singles) { txn in
                                    CardPlacedBet(txn: txn, onCancel: nil)
                                }

                                ForEach(group.parlays, id: \.first?.parlayId) { legs in
                                    CardPlacedParlay(legs: legs, onCancel: nil)
                                }
                            }
                        }
                    }
                }
                .padding(.top, 8)

            }
            .padding(.horizontal, 16)
            .padding(.top, 16)
            .padding(.bottom, 32)
        }
    }

    private func loadHistory() async {
        guard bettorId != 0 else { return }
        isLoadingHistory = completedRecords.isEmpty
        defer { isLoadingHistory = false }
        completedRecords = (try? await txnService.fetchCompletedBets(bettorId: bettorId)) ?? []
        let ids = Set(completedRecords.map(\.syndicateId))
        for sid in ids where historySyndicates[sid] == nil {
            if let result = try? await syndicateService.fetchSyndicate(syndicateId: sid, bettorId: nil) {
                historySyndicates[sid] = result.first
            }
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
        "figure.equestrian.sports.circle.fill",
        "person.circle.fill",
        "person.fill",
        "star.circle.fill",
        "star.fill",
        "flame.circle.fill",
        "flame.fill",
        "bolt.circle.fill",
        "bolt.fill",
        "crown.fill",
        "trophy.fill",
        "target",
        "gamecontroller.fill",
        "pawprint.circle.fill",
        "pawprint.fill",
        "leaf.fill",
        "moon.stars.fill",
        "sun.max.fill",
        "cloud.bolt.fill",
        "drop.fill",
        "heart.fill",
        "shield.fill",
        "flag.fill",
        "medal.fill",
        "rosette",
        "sparkles",
        "bell.fill",
        "gift.fill",
        "suit.heart.fill",
        "suit.club.fill",
        "suit.spade.fill",
        "suit.diamond.fill"
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
