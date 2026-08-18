import SwiftUI

/// Unit-history breakdown shown under a `CardProfile`: one section of per-syndicate/runner
/// balances, one section of per-league/sport balances. Shared by `TabProfileView` (own
/// profile) and `SheetRunner` (another syndicate runner's profile).
struct CardUnitBreakdown: View {
    @EnvironmentObject private var theme: AppTheme
    @Environment(\.colorScheme) private var colorScheme

    let syndicateRunners: [Runner]
    var syndicates: [Syndicate] = []
    let leagueBalances: [BettorLeagueBalance]
    let leagues: [League]

    private func league(for id: Int) -> League? { leagues.first { $0.id == id } }
    private func syndicate(for id: Int) -> Syndicate? { syndicates.first { $0.syndicateId == id } }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            if !syndicateRunners.isEmpty {
                section(title: "Syndicates", rows: syndicateRunners) { runner in
                    syndicateRow(runner)
                }
            }

            if !leagueBalances.isEmpty {
                section(title: "Leagues", rows: leagueBalances) { lb in
                    row(icon: League.sportIcon(for: lb.leagueId), label: league(for: lb.leagueId)?.abbr ?? "League", value: lb.balance)
                }
            }
        }
    }

    @ViewBuilder
    private func section<Item: Identifiable, RowContent: View>(
        title: String,
        rows: [Item],
        @ViewBuilder rowContent: @escaping (Item) -> RowContent
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)

            VStack(spacing: 0) {
                ForEach(Array(rows.enumerated()), id: \.element.id) { index, item in
                    rowContent(item)
                    if index < rows.count - 1 {
                        Divider().padding(.horizontal, 16)
                    }
                }
            }
            .background(theme.cardBackground(colorScheme))
            .clipShape(RoundedRectangle(cornerRadius: 14))
        }
    }

    private func syndicateRow(_ runner: Runner) -> some View {
        let syn = syndicate(for: runner.syndicateId)
        return HStack(spacing: 12) {
            Image(systemName: syn?.symbol ?? "house.fill")
                .foregroundStyle(ProfileOption.color(for: syn?.color ?? ""))
                .frame(width: 20)

            VStack(alignment: .leading, spacing: 2) {
                Text(syn?.name ?? "Syndicate \(runner.syndicateId)")
                    .foregroundStyle(theme.primaryText(colorScheme))

                HStack(spacing: 4) {
                    Image(systemName: runner.symbol ?? "person.fill")
                    Text(runner.profileName ?? "Runner")
                }
                .font(.caption)
                .foregroundStyle(.secondary)
            }

            Spacer()
            unitValue(runner.balance ?? 0)
        }
        .padding()
    }

    private func row(icon: String?, label: String, value: Double) -> some View {
        HStack {
            if let icon {
                Image(systemName: icon)
                    .foregroundStyle(.secondary)
            }
            Text(label)
                .foregroundStyle(theme.primaryText(colorScheme))
            Spacer()
            unitValue(value)
        }
        .padding()
    }

    private func unitValue(_ value: Double) -> some View {
        let tint = value >= 0 ? theme.win : theme.loss
        let sign = value >= 0 ? "+" : ""
        return HStack(spacing: 3) {
            Text("\(sign)\(txnWagerLabel(value))")
                .font(.subheadline.weight(.semibold))
            Image(systemName: "nairasign.circle.fill")
        }
        .foregroundStyle(tint)
    }
}

#Preview("CardUnitBreakdown") {
    ScrollView {
        CardUnitBreakdown(
            syndicateRunners: Mock.runners,
            syndicates: [Mock.syndicate],
            leagueBalances: [
                BettorLeagueBalance(bettorId: 42, leagueId: 2, balance: 42),
                BettorLeagueBalance(bettorId: 42, leagueId: 1, balance: -12)
            ],
            leagues: [Mock.leagueNBA, Mock.leagueNFL]
        )
        .padding()
    }
    .environmentObject(AppTheme())
}
