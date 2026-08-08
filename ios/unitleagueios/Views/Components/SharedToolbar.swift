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

// MARK: - RowCapsuleButton

struct RowCapsuleButton: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let systemName: String
    let isSelected: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Image(systemName: systemName)
                .font(.subheadline.weight(isSelected ? .semibold : .regular))
                .foregroundStyle(isSelected ? theme.accent : theme.primaryText(colorScheme))
                .padding(.horizontal, 14)
                .padding(.vertical, 6)
                .background(isSelected ? theme.accent.opacity(0.15) : theme.chipUnselected(colorScheme))
                .clipShape(Capsule())
        }
    }
}

// MARK: - SegmentedToggle

struct SegmentedToggle: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let leftLabel: String
    let rightLabel: String
    @Binding var isRightSelected: Bool

    var body: some View {
        HStack(spacing: 2) {
            segment(leftLabel, selected: !isRightSelected) { isRightSelected = false }
            segment(rightLabel, selected: isRightSelected) { isRightSelected = true }
        }
        .padding(3)
        .background(theme.chipUnselected(colorScheme))
        .clipShape(Capsule())
    }

    @ViewBuilder
    private func segment(_ label: String, selected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Text(label)
                .font(.subheadline.weight(selected ? .semibold : .regular))
                .foregroundStyle(selected ? theme.chipSelectedFG(colorScheme) : theme.primaryText(colorScheme))
                .padding(.horizontal, 14)
                .padding(.vertical, 6)
                .background(selected ? theme.chipSelected(colorScheme) : Color.clear)
                .clipShape(Capsule())
        }
        .buttonStyle(.plain)
    }
}

// MARK: - StreakText

struct StreakText: View {
    @EnvironmentObject private var theme: AppTheme
    let streak: String
    let positiveChar: Character
    var font: Font = .caption2.weight(.semibold)

    var body: some View {
        HStack(spacing: 1) {
            ForEach(Array(streak.enumerated()), id: \.offset) { _, ch in
                Text(String(ch))
                    .font(font)
                    .foregroundStyle(ch == positiveChar ? theme.win : theme.loss)
            }
        }
    }
}

// MARK: - TrendStreakText

/// Like `StreakText`, but each source character maps to its own display glyph
/// and an optional color (nil falls back to `.secondary`) via `mapping`.
struct TrendStreakText: View {
    @EnvironmentObject private var theme: AppTheme
    let streak: String
    let mapping: (Character) -> (display: String, color: Color?)
    var font: Font = .caption2.weight(.semibold)
    var charWidth: CGFloat = 11

    var body: some View {
        HStack(spacing: 0) {
            ForEach(Array(streak.enumerated()), id: \.offset) { _, ch in
                let (display, color) = mapping(ch)
                Text(display)
                    .font(font.monospaced())
                    .foregroundStyle(color ?? .secondary)
                    .frame(width: charWidth)
            }
        }
    }
}

// MARK: - SyndicateHeaderRow

struct SyndicateHeaderRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    let syndicate: Syndicate?
    let syndicateId: Int
    let runner: Runner?
    let bettorId: Int

    var body: some View {
        HStack(spacing: 6) {
            Image(systemName: syndicate?.symbol ?? "house.fill")
                .font(.caption.weight(.semibold))
                .foregroundStyle(ProfileOption.color(for: syndicate?.color ?? ""))
            Text(syndicate?.name ?? "Syndicate \(syndicateId)")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            if let runner {
                Text("-")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Image(systemName: runner.symbol ?? "person.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Text(runner.profileName ?? "Runner \(bettorId)")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 4)
    }
}

// MARK: - DateNavigationHeader

struct DateNavigationHeader: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme
    @Binding var selectedDate: Date
    /// When non-nil (and non-empty), prior/next navigation jumps to the nearest earlier/later
    /// date in this set instead of stepping by a plain calendar day.
    var validDates: Set<Date>? = nil
    @State private var showDatePicker = false

    private let displayFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "EEE, MMM d"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f
    }()

    private func step(_ direction: Int) -> Date {
        guard let validDates, !validDates.isEmpty else {
            return Calendar.current.date(byAdding: .day, value: direction, to: selectedDate) ?? selectedDate
        }
        let sortedDates = validDates.sorted()
        if direction > 0 {
            return sortedDates.first(where: { $0 > selectedDate }) ?? selectedDate
        } else {
            return sortedDates.last(where: { $0 < selectedDate }) ?? selectedDate
        }
    }

    private var prevDayNumber: Int {
        Calendar.current.component(.day, from: step(-1))
    }

    private var nextDayNumber: Int {
        Calendar.current.component(.day, from: step(1))
    }

    var body: some View {
        HStack(spacing: 12) {
            Button {
                selectedDate = step(-1)
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
                SharedDatePickerSheet(selectedDate: $selectedDate, validDates: validDates)
            }

            Button {
                selectedDate = step(1)
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
    var validDates: Set<Date>? = nil
    @Environment(\.dismiss) private var dismiss

    private func nearestValidDate(to date: Date) -> Date {
        guard let validDates, !validDates.isEmpty else { return date }
        return validDates.min(by: { abs($0.timeIntervalSince(date)) < abs($1.timeIntervalSince(date)) }) ?? date
    }

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
                    Button("Done") {
                        selectedDate = nearestValidDate(to: selectedDate)
                        dismiss()
                    }
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
    @State private var showingBalances = false
    @State private var currentRunner: Runner?

    private let runnerService = RunnerService()

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

    private var displayedBalance: Int {
        selectedSyndicateId == 0 ? userUnits : Int((currentRunner?.balance ?? 0).rounded())
    }

    private var profileTrailingItem: some View {
        Button { showingBalances = true } label: {
            HStack(spacing: 10) {
                HStack(spacing: 3) {
                    Image(systemName: "nairasign.circle.fill")
                    Text("\(displayedBalance)").fontWeight(.semibold)
                }
                .font(.subheadline)
                .foregroundStyle(theme.primaryText(colorScheme))

                Image(systemName: profileSymbol)
                    .font(.title2)
                    .foregroundStyle(theme.accent)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
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
            .sheet(isPresented: $showingBalances) {
                SheetSyndicateBalances(bettorId: bettorId)
            }
            .task(id: "\(bettorId)-\(selectedSyndicateId)") { await loadRunner() }
    }

    private func loadRunner() async {
        guard bettorId != 0, selectedSyndicateId != 0 else { currentRunner = nil; return }
        currentRunner = (try? await runnerService.fetchRunner(bettorId: bettorId, syndicateId: selectedSyndicateId))?.first
    }
}

// MARK: - SyndicateSelectorSheet



extension View {
    func tabToolbar() -> some View {
        modifier(TabToolbar())
    }
}
