import SwiftUI

enum AccentOption: String, CaseIterable, Identifiable {
    case green   = "green"
    case red     = "red"
    case yellow  = "yellow"
    case stadium = "stadium"
    case teal    = "teal"

    var id: String { rawValue }

    var color: Color {
        switch self {
        case .green:   return Color(hex: "9FAA67")
        case .red:     return Color(hex: "C7543E")
        case .yellow:  return Color(hex: "D8B061")
        case .stadium: return Color(hex: "7B5EA7")
        case .teal:    return Color(hex: "4ECDC4")
        }
    }

    var label: String {
        switch self {
        case .green:   return "Sage"
        case .red:     return "Clay"
        case .yellow:  return "Amber"
        case .stadium: return "Stadium"
        case .teal:    return "Teal"
        }
    }
}

final class AppTheme: ObservableObject {
    var accentOption: AccentOption = AccentOption(rawValue: UserDefaults.standard.string(forKey: "accentOption") ?? "green") ?? .green {
        willSet { objectWillChange.send() }
        didSet  { UserDefaults.standard.set(accentOption.rawValue, forKey: "accentOption") }
    }

    var accent: Color { accentOption.color }

    func appBackground(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? Color(white: 0.04) : Color(white: 0.96)
    }

    func cardBackground(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? Color.white.opacity(0.07) : Color.black.opacity(0.05)
    }

    func cardBackgroundProminent(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? Color.white.opacity(0.12) : Color.black.opacity(0.10)
    }

    func divider(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? Color.white.opacity(0.10) : Color.black.opacity(0.10)
    }

    func primaryText(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? .white : .black
    }

    func chipSelected(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? .white : .black
    }

    func chipSelectedFG(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? .black : .white
    }

    func chipUnselected(_ scheme: ColorScheme) -> Color {
        scheme == .dark ? Color.white.opacity(0.10) : Color.black.opacity(0.10)
    }

    var win:   Color { Color(hex: "9FAA67") }
    var loss:  Color { Color(hex: "C7543E") }
    var error: Color { Color(hex: "C7543E") }
}

private extension Color {
    init(hex: String) {
        let hex = hex.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
        var int: UInt64 = 0
        Scanner(string: hex).scanHexInt64(&int)
        let r = Double((int >> 16) & 0xFF) / 255
        let g = Double((int >> 8)  & 0xFF) / 255
        let b = Double(int         & 0xFF) / 255
        self.init(red: r, green: g, blue: b)
    }
}
