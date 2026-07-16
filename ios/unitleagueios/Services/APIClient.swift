import Foundation

enum APIClient {
    static let localURL  = "http://192.168.4.59:8000"
    static let remoteURL = "https://api.unitleague.com"

    static var baseURL: String {
        UserDefaults.standard.bool(forKey: "useLocalAPI") ? localURL : remoteURL
    }
}
