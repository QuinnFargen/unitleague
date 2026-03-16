import requests
import pandas as pd
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}

URLS = {
    "NFL": "https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "CFB": "https://site.web.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
}


def get_calendar(url, league, year):

    r = requests.get(url, headers=HEADERS, params={"dates": f"{year}0801"})
    data = r.json()

    rows = []

    cal = data["leagues"][0]["calendar"]

    for season in cal:

        season_label = season["label"]
        season_value = season["value"]

        entries = season.get("entries", [])

        if not entries:
            continue

        for entry in entries:
        # for entry in season["entries"]:

            rows.append({
                "league": league,
                "yr": year,
                "season_label": season_label,
                "season_value": season_value,
                "week_label": entry["label"],
                "week_value": entry["value"],
                "week_start_date": entry["startDate"],
                "week_end_date": entry["endDate"]
            })

    return rows


def collect_calendars(start_year, end_year):

    rows = []

    for year in range(start_year, end_year + 1):

        print(f"Collecting {year}")

        for league, url in URLS.items():

            rows.extend(get_calendar(url, league, year))
            time.sleep(1)


    df = pd.DataFrame(rows)

    df["week_start_date"] = pd.to_datetime(df["week_start_date"])
    df["week_end_date"] = pd.to_datetime(df["week_end_date"])

    return df


def main():

    df = collect_calendars(2015, 2026)

    df = df.drop_duplicates()

    df.to_csv("football_weeks.csv", index=False)

    print("Saved football_weeks.csv")


main()


