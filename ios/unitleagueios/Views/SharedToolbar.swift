import SwiftUI

// MARK: - FilterChip

struct FilterChip: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let label: String
    let isSelected: Bool
    var availabilityTint: Color? = nil
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Text(label)
                .font(.subheadline.weight(isSelected ? .semibold : .regular))
                .foregroundStyle(isSelected ? theme.chipSelectedFG(colorScheme) : theme.primaryText(colorScheme))
                .padding(.horizontal, 14)
                .padding(.vertical, 6)
                .background(isSelected ? theme.chipSelected(colorScheme) : theme.chipUnselected(colorScheme))
                .clipShape(Capsule())
                .overlay(
                    Capsule()
                        .strokeBorder(availabilityTint ?? .clear, lineWidth: 1.5)
                )
        }
    }
}

// MARK: - DateNavigationHeader

struct DateNavigationHeader: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Binding var selectedDate: Date
    @State private var showDatePicker = false

    private let displayFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "EEE, MMM d"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private var prevDayNumber: Int {
        let prev = Calendar.current.date(byAdding: .day, value: -1, to: selectedDate) ?? selectedDate
        return Calendar.current.component(.day, from: prev)
    }

    private var nextDayNumber: Int {
        let next = Calendar.current.date(byAdding: .day, value: 1, to: selectedDate) ?? selectedDate
        return Calendar.current.component(.day, from: next)
    }

    var body: some View {
        HStack(spacing: 12) {
            Button {
                selectedDate = Calendar.current.date(byAdding: .day, value: -1, to: selectedDate) ?? selectedDate
            } label: {
                HStack {
                    Image(systemName: "chevron.left").font(.title3.weight(.semibold))
                    Image(systemName: "\(prevDayNumber).calendar").font(.title3.weight(.semibold))
                }
                .foregroundStyle(theme.primaryText(colorScheme))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(theme.cardBackground(colorScheme))
            .clipShape(Capsule())

            Button { showDatePicker = true } label: {
                Text(displayFormatter.string(from: selectedDate))
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
            }
            .sheet(isPresented: $showDatePicker) {
                SharedDatePickerSheet(selectedDate: $selectedDate)
            }

            Button {
                selectedDate = Calendar.current.date(byAdding: .day, value: 1, to: selectedDate) ?? selectedDate
            } label: {
                HStack {
                    Image(systemName: "\(nextDayNumber).calendar").font(.title3.weight(.semibold))
                    Image(systemName: "chevron.right").font(.title3.weight(.semibold))
                }
                .foregroundStyle(theme.primaryText(colorScheme))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(theme.cardBackground(colorScheme))
            .clipShape(Capsule())

            Button("Today") { selectedDate = .now }
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(theme.primaryText(colorScheme))
                .padding(.horizontal, 12)
                .padding(.vertical, 6)
                .background(theme.cardBackground(colorScheme))
                .clipShape(Capsule())
        }
        .padding(.horizontal)
        .padding(.vertical, 10)
    }
}

// MARK: - SharedDatePickerSheet

private struct SharedDatePickerSheet: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Binding var selectedDate: Date
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ZStack {
                theme.appBackground(colorScheme).ignoresSafeArea()
                DatePicker("", selection: $selectedDate, displayedComponents: .date)
                    .datePickerStyle(.graphical)
                    .tint(theme.accent)
                    .padding(.horizontal)
            }
            .navigationTitle("Select Date")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}

// MARK: - TabToolbar

struct TabToolbar: ViewModifier {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @AppStorage("profileSymbol")       private var profileSymbol: String       = ProfileOption.symbols[0]
    @AppStorage("leagueSymbol")        private var leagueSymbol: String        = "person.circle.fill"
    @AppStorage("leagueColorName")     private var leagueColorName: String     = AccentOption.allCases[0].rawValue
    @AppStorage("userUnits")           private var userUnits: Int              = 100
    @AppStorage("bettorId")            private var bettorId: Int               = 0
    @AppStorage("selectedSyndicateId") private var selectedSyndicateId: Int    = 0
    @AppStorage("leagueRank")          private var leagueRank: Int             = 0
    @State private var showingSyndicateSelector = false
    @State private var showingBookmarks = false

    private func rankLabel(_ rank: Int) -> String {
        switch rank {
        case 1:  return "1st"
        case 2:  return "2nd"
        case 3:  return "3rd"
        case let n where n > 3: return "\(n)th"
        default: return "Last"
        }
    }

    private var leagueLeadingItem: some View {
        Button { showingSyndicateSelector = true } label: {
            HStack(spacing: 6) {
                Image(systemName: leagueSymbol)
                    .font(.title2)
                    .foregroundStyle(ProfileOption.color(for: leagueColorName))

                Text(rankLabel(leagueRank))
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(theme.primaryText(colorScheme))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
//            .background(theme.cardBackground(colorScheme))
            .clipShape(Capsule())
        }
        .buttonStyle(.plain)
        .fixedSize()
    }

    private var profileTrailingItem: some View {
        Button { showingBookmarks = true } label: {
            HStack(spacing: 10) {
                HStack(spacing: 3) {
                    Image(systemName: "nairasign.circle.fill")
                    Text("\(userUnits)").fontWeight(.semibold)
                }
                .font(.subheadline)
                .foregroundStyle(theme.primaryText(colorScheme))

                Image(systemName: profileSymbol)
                    .font(.title2)
                    .foregroundStyle(theme.accent)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
//            .background(theme.cardBackground(colorScheme))
            .clipShape(Capsule())
        }
        .buttonStyle(.plain)
        .fixedSize()
    }

    func body(content: Content) -> some View {
        content
            .navigationTitle("")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    leagueLeadingItem
                }
                ToolbarItem(placement: .principal) {
                    Image("UNIT_Logo")
                        .resizable()
                        .scaledToFit()
                        .frame(height: 100)
                }
                ToolbarItem(placement: .navigationBarTrailing) {
                    profileTrailingItem
                }
            }
            .sheet(isPresented: $showingSyndicateSelector) {
                SheetSyndicateSelector(
                    bettorId: bettorId,
                    selectedSyndicateId: $selectedSyndicateId,
                    leagueSymbol: $leagueSymbol,
                    leagueColorName: $leagueColorName,
                    leagueRank: $leagueRank
                )
            }
            .sheet(isPresented: $showingBookmarks) {
                SheetBookmarks()
            }
    }
}

// MARK: - SyndicateSelectorSheet



extension View {
    func tabToolbar() -> some View {
        modifier(TabToolbar())
    }
}
