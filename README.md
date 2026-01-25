# unitleague

Project to collect sports data to work with postgres, dbt & kestra. Plan is to collect boxscore data, betting odds, weather & other meta data. Using dbt for feature engineering datasets to utilize for predicting betting models. Utilizing kestra to orchestrate the data collection into postgres & dbt models builds. End goal of making a interface to allow for what I'm calling unit league. This would be an fantasy football alternative with a fake currency to see who has the most units at the end of the season from hypothetical bets. 

I do NOT want to encourage actual sports betting. 
- This will serve as an alternative without the option to lose or win money. 
- This will help track how bad the average better is and explain how long the odds of parleys are.
- There will be an attempt to educate the realities of the gaming industry:
  - [Breaking Points - Gambling Lies](https://youtu.be/jSxZUw923gs?si=1yvXf8iRPnW27rF9)
  - [Breaking Points - Gambling Crisis](https://youtu.be/qJw7lIO9KeE?si=d7gioEkhcZUkUYmo)


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
