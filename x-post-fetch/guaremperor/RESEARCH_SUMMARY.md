# GuarEmperor post retrieval and Notion access probe

## Post 2089713373659447783

Created at: 2026-08-18 13:57:47 UTC

Text:

> Fullset tools for screening
>
> NFTs: MintGo, Waypoint MintScan, MCT NFT Minting, GUAP, AlphaTrack.
>
> Feed Trending: Leak, PureAlpha, 985 Monitor, Uxento, X Relay, J7 Tracker, Wind.
>
> Checking Larp/Scam Project: JustLarps.
>
> Early Project: Moni, AlphaGate, Orbital.
>
> Tracker activity CT: SilentAlphaBot, Redacted Systems bot.

## Post 2092954898434490799

Created at: 2026-08-27 12:38:27 UTC

Text summary:

- Wallets NFTs Trackers: Notion database `b8ca8c3741d74d709e6c8e17c254818d`.
- Wallet Pons meme: Notion page `d6e701daad2d4b5dad3968edbcb4d5d1`.
- Wallet Arc meme: Notion database `d0afecae9b1445fc8f73f1b44b43b75c`.
- Wallet Stable meme: Notion database `80f2d75809244415bae1ff4b48d78cf1`.

Attached screenshots show:

- A structured wallet JSON with fields including `badge`, `tags`, `stats`, `copyScore`, `score`, `realizedUsd`, `tokensEarly`, `runnersCaught`, `activeTrader`, and `activeDaysAgo`.
- A bulk address list.
- Wallet records with tags and aggregate fields such as `buys`, `sells`, `tokens`, `spentUsd`, `recvUsd`, and `netUsd`.
- A large KOL-labelled address list.

## Anonymous Notion access probe

All four page URLs returned HTTP 200 HTML shells titled `Notion`, but the HTML contained no wallet addresses or database identifiers. A direct anonymous request to Notion's `getPublicPageData` endpoint returned:

```json
{"publicAccessRole":"none"}
```

for every page.

Interpretation: the links are not anonymously exportable through the public page API. Automated collection requires either an authenticated Notion session with access, or a CSV/JSON export supplied by an authorized user. The `app.notion.com/library/recents` Pons link is specifically tied to a Notion workspace/session rather than a normal public page.

## Source handling

The post JSON and attached media were retrieved through the public FxTwitter JSON endpoint in a temporary, read-only GitHub Actions workflow. No branch was merged into `main`.
