import SwiftUI

/// Building blocks shared by the Learn pages (`ViewLearnAbout`, `ViewLearnUnitLeague`,
/// `ViewLearnTerms`, `ViewLearnResource`, `ViewLearnFAQ`). Content mirrors `web/main/app/learn`.
enum LearnSection: String, CaseIterable, Identifiable {
    case about, unitLeague, terms, resource, faq

    var id: String { rawValue }

    var label: String {
        switch self {
        case .about:      return "About"
        case .unitLeague: return "Unit League"
        case .terms:      return "Terms"
        case .resource:   return "Resource"
        case .faq:        return "FAQ"
        }
    }

    var icon: String {
        switch self {
        case .about:      return "info.circle.fill"
        case .unitLeague: return "trophy.fill"
        case .terms:      return "character.book.closed.fill"
        case .resource:   return "lifepreserver.fill"
        case .faq:        return "questionmark.circle.fill"
        }
    }

    @ViewBuilder
    var destination: some View {
        switch self {
        case .about:      ViewLearnAbout()
        case .unitLeague: ViewLearnUnitLeague()
        case .terms:      ViewLearnTerms()
        case .resource:   ViewLearnResource()
        case .faq:        ViewLearnFAQ()
        }
    }
}

/// Scrolling page shell: themed background, lede line, then the page body.
struct LearnPage<Content: View>: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let title: String
    let lede: String
    @ViewBuilder let content: Content

    var body: some View {
        ZStack {
            theme.appBackground(colorScheme).ignoresSafeArea()

            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    LearnText(lede)
                        .font(.title3)
                        .foregroundStyle(.secondary)
                    content
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.horizontal, 16)
                .padding(.top, 16)
                .padding(.bottom, 32)
            }
        }
        .navigationTitle(title)
        .navigationBarTitleDisplayMode(.large)
    }
}

/// Body text with inline markdown (`**bold**`, `*italic*`, `[link](url)`).
struct LearnText: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let markdown: String

    init(_ markdown: String) { self.markdown = markdown }

    var body: some View {
        Text(Self.attributed(markdown))
            .foregroundStyle(theme.primaryText(colorScheme))
            .tint(theme.accent)
            .fixedSize(horizontal: false, vertical: true)
    }

    static func attributed(_ markdown: String) -> AttributedString {
        let options = AttributedString.MarkdownParsingOptions(interpretedSyntax: .inlineOnlyPreservingWhitespace)
        return (try? AttributedString(markdown: markdown, options: options)) ?? AttributedString(markdown)
    }
}

struct LearnHeading: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let text: String
    var level: Int = 2

    init(_ text: String, level: Int = 2) {
        self.text = text
        self.level = level
    }

    var body: some View {
        Text(text)
            .font(level == 2 ? .title2.weight(.bold) : .headline)
            .foregroundStyle(theme.primaryText(colorScheme))
            .padding(.top, level == 2 ? 10 : 4)
    }
}

struct LearnBullets: View {
    @EnvironmentObject private var theme: AppTheme

    let items: [String]

    init(_ items: [String]) { self.items = items }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            ForEach(items, id: \.self) { item in
                HStack(alignment: .firstTextBaseline, spacing: 10) {
                    Circle()
                        .fill(theme.accent)
                        .frame(width: 6, height: 6)
                        .alignmentGuide(.firstTextBaseline) { $0[.bottom] + 1 }
                    LearnText(item)
                }
            }
        }
    }
}

/// Accent-edged card used for asides.
struct LearnCallout: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let paragraphs: [String]

    init(_ paragraphs: String...) { self.paragraphs = paragraphs }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            ForEach(paragraphs, id: \.self) { LearnText($0) }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding()
        .background(theme.cardBackground(colorScheme))
        .overlay(alignment: .leading) {
            Rectangle().fill(theme.accent).frame(width: 4)
        }
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

/// Simple grid table: a header row followed by data rows.
struct LearnTable: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let header: [String]
    let rows: [[String]]

    var body: some View {
        Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 10) {
            GridRow {
                ForEach(header, id: \.self) { cell in
                    Text(cell)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                }
            }
            Divider()
            ForEach(rows, id: \.self) { row in
                GridRow {
                    ForEach(Array(row.enumerated()), id: \.offset) { index, cell in
                        Text(cell)
                            .font(index == 0 ? .footnote.weight(.semibold) : .footnote)
                            .foregroundStyle(theme.primaryText(colorScheme))
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
            }
        }
        .padding()
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(theme.cardBackground(colorScheme))
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }
}

/// Card-style navigation row (icon, title, chevron) used for links on `TabProfileView` and Learn pages.
struct NavCardRow: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let icon: String
    let title: String

    var body: some View {
        HStack {
            Image(systemName: icon)
                .foregroundStyle(theme.accent)
                .frame(width: 24)
            Text(title)
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
}
