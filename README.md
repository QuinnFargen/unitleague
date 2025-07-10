# Unit League

Project to collect sports data to work with postgres, dbt, kestra & streamlit. Plan is to collect boxscore data, betting odds, weather & other meta data. Using dbt for feature engineering datasets to utilize postgresML for predicting betting models. Utilizing kestra to orchestrate the data collection into postgres & dbt models builds. End goal of making a stremlit interface to allow for what I'm calling unit league. This would be an fantasy football alternative witha fake currancy to see who has the most units at the end of the season from hypothetical bets. 

---

## Goals

### 1. Historical Data

- Box scores (team and player metrics)  
- Game-level outcomes with scoring breakdowns by quarter/period  
- Betting lines (spread, total, moneyline)  
- Meta data (coach, referee, weather)

### 2. Bet & Analyst Tracking

- Manual bet logging interface (supports analyst attributions via links to tweets or podcasts)  
- User-based betting history  
- System & trend tracking (e.g., unders after back-to-back travel)  
- Prediction tracking for models and individual analysts

### 3. Fantasy-Style Betting Leagues

- Add analysts to a private or public league  
- Track head-to-head results over time  
- Scoring models to simulate league standings
- Team-level betting predictions with optional social media automation  

---

## Data Grains

- **Game Results**  
  - Outcomes, scoring by period/qtr/inn
  - Betting odds (spread, total, moneyline)
  - Analyst/Pundit betting picks tracked

- **Box Scores**  
  - Team stats
  - Player stats (rebounds, assists, etc.)
  - Meta Data (weather, time, coach, ref)

- **Feature Engineered** 
  - dbt models
  - Trends & Patterns
  - Season running avg

- **Play-by-Play Data** 
  - Unlikely this granular

---

## Roadmap

- [x] Historic Betting Odds Collected (2020)
- [ ] Historic Games & Boxscores Collected (2011)
- [x] Automate Odds & Weather Collection
- [ ] Automate Game Outcome & Boxscore Collection
- [x] Automate Odds Collection
- [ ] Setup dbt for feature & trend datasets
- [ ] Setup postgresML extention for betting predictions
- [ ] Finalize Streamlit dashboard for fantasy-style league interface  
- [ ] Automate Analyst/Podcast betting picks into league
- [ ] Social media automated trends and picks posted
