import AuthenticationServices
import UIKit

/// Re-runs Sign in with Apple to get a fresh one-time authorizationCode, which the API
/// exchanges for a refresh token so it can revoke it when the account is deleted.
@MainActor
final class AppleReauthorizer: NSObject, ASAuthorizationControllerDelegate, ASAuthorizationControllerPresentationContextProviding {
    private var continuation: CheckedContinuation<String?, Error>?

    func authorizationCode() async throws -> String? {
        let request = ASAuthorizationAppleIDProvider().createRequest()
        let controller = ASAuthorizationController(authorizationRequests: [request])
        controller.delegate = self
        controller.presentationContextProvider = self
        return try await withCheckedThrowingContinuation { continuation in
            self.continuation = continuation
            controller.performRequests()
        }
    }

    nonisolated func authorizationController(controller: ASAuthorizationController, didCompleteWithAuthorization authorization: ASAuthorization) {
        let code = (authorization.credential as? ASAuthorizationAppleIDCredential)?
            .authorizationCode
            .flatMap { String(data: $0, encoding: .utf8) }
        Task { @MainActor in
            continuation?.resume(returning: code)
            continuation = nil
        }
    }

    nonisolated func authorizationController(controller: ASAuthorizationController, didCompleteWithError error: Error) {
        Task { @MainActor in
            continuation?.resume(throwing: error)
            continuation = nil
        }
    }

    nonisolated func presentationAnchor(for controller: ASAuthorizationController) -> ASPresentationAnchor {
        MainActor.assumeIsolated {
            UIApplication.shared.connectedScenes
                .compactMap { $0 as? UIWindowScene }
                .flatMap(\.windows)
                .first { $0.isKeyWindow } ?? ASPresentationAnchor()
        }
    }
}

enum LocalAccountData {
    /// Developer toggles that survive account deletion.
    private static let keptKeys: Set<String> = ["useLocalAPI"]

    /// Removes every app-owned UserDefaults key (all @AppStorage values, cached
    /// syndicate/league data, bookmarks, accent). Per-key removal notifies @AppStorage
    /// observers, so views fall back to their signed-out defaults immediately.
    static func wipe() {
        let defaults = UserDefaults.standard
        guard let domain = Bundle.main.bundleIdentifier,
              let keys = defaults.persistentDomain(forName: domain)?.keys
        else { return }
        for key in keys where !keptKeys.contains(key) {
            defaults.removeObject(forKey: key)
        }
    }
}
