"use client";

import { useState } from "react";
import Link from "next/link";

// Decision tree. Question nodes have `options` pointing to the next node id;
// result nodes have `result`. An option's `admits` is the downside or loophole
// the user accepted by picking it; "could use" results list them back. Options
// sharing an `admitsKey` collapse to the latest one (e.g. the losing-streak loop).
const TILT_STEP = 3;
const TILT_MAX = 15;

const tiltNodes = {};
for (let days = TILT_STEP; days <= TILT_MAX; days += TILT_STEP) {
  tiltNodes[`tilt${days}`] = {
    step: "Tilt",
    question: `You've lost ${days} days in a row. What's your instinct?`,
    options: [
      {
        label: "Bet bigger to win it back",
        detail: "One good hit gets me even.",
        next: "resultChase",
      },
      {
        label: "Stop for the day",
        detail: "Losses are part of it.",
        next: days + TILT_STEP <= TILT_MAX ? `tilt${days + TILT_STEP}` : "resultBook",
        admitsKey: "tilt",
        admits: `Sitting through a ${days}-day losing streak with real money`,
      },
    ],
  };
}

const NODES = {
  start: {
    step: "Why",
    question: "What do you want out of betting on sports?",
    options: [
      { label: "Fun with friends", detail: "Picks, trash talk, and a leaderboard.", next: "needMoney" },
      {
        label: "To make money",
        detail: "I think I can profit over time.",
        next: "vig",
        admits: "Treating betting as income, when the house takes a cut of every bet",
      },
    ],
  },
  needMoney: {
    step: "Stakes",
    question: "Would it still be fun if no money was on the line?",
    options: [
      { label: "Yes, the competition is the fun", detail: "Beating my friends is enough.", next: "resultLeague" },
      {
        label: "No, I need real money to sweat it",
        detail: "It doesn't feel real without it.",
        next: "budget",
        admits: "Needing real money at risk to enjoy the game",
      },
    ],
  },
  vig: {
    step: "The math",
    question: "At -110 odds you need to win 52.4% of your bets just to break even. Most bettors don't. Can you beat that over thousands of bets?",
    options: [
      {
        label: "I think I can",
        detail: "I've got a system or a model.",
        next: "limits",
        admits: "Needing to win more than 52.4% of bets just to break even",
      },
      { label: "I didn't know that", detail: "That's a higher bar than I expected.", next: "resultLearn" },
    ],
  },
  limits: {
    step: "Limits",
    question: "If you win consistently, sportsbooks will limit your max bet, sometimes to a few dollars. What would you do?",
    options: [
      { label: "Bet through a friend's account", detail: "They can't limit me if it isn't my account.", next: "resultBearding" },
      {
        label: "Move to a prediction market",
        detail: "Trade against people, not the house.",
        next: "model",
        admits: "Switching platforms to get around being limited",
      },
      { label: "Prove my edge first", detail: "Track my picks without risking money.", next: "resultLeague" },
    ],
  },
  model: {
    step: "Model",
    question: "Market makers on prediction markets price with data and models. Do you have a model built on data?",
    options: [
      {
        label: "Yes, a data model",
        detail: "Built on stats, odds history, or both.",
        next: "backtest",
        admits: "Building and maintaining a model to compete with professional market makers",
      },
      { label: "No, research and gut feel", detail: "I watch a lot of games.", next: "resultNoModel" },
    ],
  },
  backtest: {
    step: "Backtest",
    question: "Have you backtested it against historical closing lines, over thousands of games, with fees included?",
    options: [
      {
        label: "Yes, it beats the close",
        detail: "It holds up after fees.",
        next: "resultMarket",
        admits: "Trusting a backtest to keep working after the market adjusts to you",
      },
      { label: "Not yet, or a small sample", detail: "It looks good so far.", next: "resultBacktest" },
    ],
  },
  budget: {
    step: "Budget",
    question: "Can you set a betting budget you could lose completely, like money spent on a concert ticket?",
    options: [
      {
        label: "Yes, and I'll stick to it",
        detail: "Money I've already written off.",
        next: "betStyle",
        admits: "Setting aside money you expect to lose",
      },
      { label: "Not really / not sure", detail: "Losing it would hurt.", next: "resultHelp" },
    ],
  },
  betStyle: {
    step: "Bet type",
    question: "What kind of bets are you drawn to?",
    options: [
      { label: "Parlays and same-game parlays", detail: "Small stake, big payout.", next: "resultParlay" },
      {
        label: "Straight bets, small stakes",
        detail: "Single games, flat sizing.",
        next: `tilt${TILT_STEP}`,
        admits: "Paying the vig on every bet, win or lose",
      },
    ],
  },
  ...tiltNodes,

  resultLeague: {
    result: {
      title: "Unit League fits you",
      body: "You get the competition, the research, and the leaderboard without the vig eating your money. If you do have an edge, a season of units will show it.",
    },
  },
  resultLearn: {
    result: {
      title: "Start with Unit League",
      body: "Play a season with units first. You'll see how the vig and variance add up before a single real dollar is at risk.",
      links: [{ href: "/learn/danger", label: "Read the math" }],
    },
  },
  resultNoModel: {
    result: {
      title: "Start with Unit League",
      body: "Without a model, you'd be trading against market makers who have one. Track your picks against real odds in Unit League and see whether your read on games actually beats the market.",
    },
  },
  resultBacktest: {
    result: {
      title: "Backtest it in Unit League",
      body: "A small sample can't tell skill from luck. Run your model's picks in Unit League for a full season against real odds. If it still wins after thousands of bets, that means something, and it cost you nothing to find out.",
      links: [{ href: "/learn/terms#sample-size", label: "Why sample size matters" }],
    },
  },
  resultMarket: {
    couldUse: "a prediction market",
    result: {
      title: "Sounds like you could use a prediction market",
      body: "You have a tested model and you know the math. But getting there meant accepting a lot.",
    },
  },
  resultBook: {
    couldUse: "a sportsbook",
    result: {
      title: "You are persistent and a terrible bettor",
      body: `You kept your discipline through a ${TILT_MAX}-day losing streak. But getting there meant accepting a lot and losing a lot.`,
    },
  },
  resultBearding: {
    danger: true,
    result: {
      title: "Stop: that's bearding",
      body: "Betting through someone else's account to dodge limits can be prosecuted as fraud, and in many states it's a felony. It puts the account holder at risk too, and books routinely void the bets and seize the balance.",
      league: "Unit League never limits winners, so there's nothing to get around. Win as much as you can.",
      links: [{ href: "/learn/danger", label: "How books catch it" }],
    },
  },
  resultHelp: {
    danger: true,
    result: {
      title: "Real money isn't a good fit right now",
      body: "If losing the money would hurt, don't put it on a game. If betting already feels hard to control, call or text 1-800-MY-RESET.",
      league: "Unit League gives you the sweat with no money on the line.",
      links: [{ href: "/learn/resource#addiction", label: "Get help" }],
    },
  },
  resultParlay: {
    danger: true,
    result: {
      title: "Parlays are where the house wins most",
      body: "Every leg adds another layer of vig. A 4-leg parlay carries about a 17% house edge, and a 10-leg parlay about 37%.",
      league: "Build all the parlays you want in Unit League. A busted ticket only costs units.",
      links: [{ href: "/learn/danger", label: "See the parlay math" }],
    },
  },
  resultChase: {
    danger: true,
    result: {
      title: "Chasing losses is a warning sign",
      body: "Raising your stakes to win back losses is how small losses turn into big ones. If the urge feels familiar, call or text 1-800-MY-RESET.",
      league: "In Unit League, a losing streak costs units, not rent.",
      links: [{ href: "/learn/resource#addiction", label: "Get help" }],
    },
  },
};

export default function Decision() {
  // Each entry is { node, choice } for a question already answered.
  const [history, setHistory] = useState([]);
  const current = history.length ? history.at(-1).choice.next : "start";
  const node = NODES[current];

  const choose = (choice) => setHistory((h) => [...h, { node: current, choice }]);
  const back = () => setHistory((h) => h.slice(0, -1));
  const restart = () => setHistory([]);

  const admitted = new Map();
  for (const { choice } of history) {
    if (choice.admits) admitted.set(choice.admitsKey ?? choice.admits, choice.admits);
  }

  return (
    <>
      {node.result ? (
        <div className={`decision-card decision-result${node.danger ? " stop" : ""}`} aria-live="polite">
          <p className="decision-step">{node.danger ? "Warning" : "Result"}</p>
          <h2>{node.result.title}</h2>
          <p>{node.result.body}</p>

          {node.couldUse && (
            <>
              <p>Unit League doesn't have the downsides and loopholes you said you'd accept:</p>
              <ul>
                {[...admitted.values()].map((a) => <li key={a}>{a}</li>)}
              </ul>
              <p>
                Skip {node.couldUse}. Play Unit League: same games, same odds, no money at risk,
                and no one limits you for winning.
              </p>
            </>
          )}

          {node.result.league && (
            <p><strong>Try Unit League instead.</strong> {node.result.league}</p>
          )}

          <p>
            {[{ href: "/learn/unit-league", label: "How Unit League works" }, ...(node.result.links ?? [])].map(
              ({ href, label }, i) => (
                <span key={href}>
                  {i > 0 && " · "}
                  <Link href={href}>{label}</Link>
                </span>
              ),
            )}
          </p>
        </div>
      ) : (
        <div className="decision-card" aria-live="polite">
          <p className="decision-step">Step {history.length + 1} · {node.step}</p>
          <h2>{node.question}</h2>
          <div className="decision-options">
            {node.options.map((opt) => (
              <button key={opt.label} type="button" className="decision-option" onClick={() => choose(opt)}>
                <strong>{opt.label}</strong>
                <span>{opt.detail}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="decision-actions">
        {history.length > 0 && (
          <button type="button" className="btn btn-outline" onClick={back}>Back</button>
        )}
        <button type="button" className="btn btn-outline" onClick={restart} disabled={history.length === 0}>
          Restart
        </button>
      </div>

      {history.length > 0 && (
        <section className="decision-log">
          <h3>Your choices so far</h3>
          <ol>
            {history.map(({ node: id, choice }) => (
              <li key={id}>{choice.label}</li>
            ))}
          </ol>
        </section>
      )}
    </>
  );
}
