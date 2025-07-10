# Unit League

Project to collect sports data to work with postgres, dbt, kestra & streamlit. Plan is to collect boxscore data, betting odds, weather & other meta data. Using dbt for feature engineering datasets to utilize postgresML for predicting betting models. Utilizing kestra to orchestrate the data collection into postgres & dbt models builds. End goal of making a stremlit interface to allow for what I'm calling unit league. This would be an fantasy football alternative witha fake currancy to see who has the most units at the end of the season from hypothetical bets. 

---

## Features

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

### 4. Predictive Models

- Team-level betting predictions with optional social media automation  
- Player prop predictions (requires integration with odds feeds)

---

## Data Structure

### Core Grains

- **Game Results**  
  - Outcomes, scoring by period

- **Box Scores**  
  - Player-level stats (rebounds, assists, etc.)

- **Team Stats**  
  - Aggregated per game or season

- **Meta Data**  
  - Coach, referee, and weather context

- **Player-Level Granularity**  
  - Supports player prop modeling

- **Play-by-Play Data** *(planned)*  
  - Fine-grained event tracking

---

## Roadmap

- [ ] Automate scraping for player props and odds  
- [ ] Finalize Streamlit dashboard for fantasy-style league interface  
- [ ] Integrate model training and evaluation  
- [ ] Add play-by-play data for select sports  
- [ ] Expand DBT models for analyst performance KPIs
