import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

AAVE_URL = "https://api.v3.aave.com/graphql"

AAVE_MARKET_QUERY = """
query GetMarket($request: MarketRequest!) {
  market(request: $request) {
    address
    chain { chainId name }
    reserves {
      underlyingToken { address symbol }
      supplyInfo { apy { value } total { value } }
      borrowInfo {
        apy { value }
        total { usdPerToken }
        utilizationRate { value }
      }
    }
  }
}
"""

ROOT = Path(__file__).resolve().parents[1]
MARKETS_PATH = ROOT / "data" / "markets.json"
LATEST_PATH = ROOT / "data" / "latest.json"

STABLE_SYMBOLS = {"USDC", "USDT", "USDE", "SUSDE", "PYUSD", "RLUSD", "USDG"}


def is_stable(symbol: str) -> bool:
    s = symbol.upper().strip()
    if s.startswith("A") and s[1:] in STABLE_SYMBOLS:
        s = s[1:]
    return s in STABLE_SYMBOLS


def is_eth(chain_id: int) -> bool:
    return chain_id == 1


def sort_markets(rows):
    def key(r):
        stable = r["isStable"]
        eth = r["isEthereum"]

        if stable and eth:
            group = 0
            primary = r["borrowApy"]
        elif stable and not eth:
            group = 1
            primary = r["borrowApy"]
        elif not stable and eth:
            group = 2
            primary = -r["supplyApy"]
        else:
            group = 3
            primary = -r["supplyApy"]

        return (group, primary, -r["tvlUsd"])

    return sorted(rows, key=key)


async def main():
    cfg = json.loads(MARKETS_PATH.read_text())
    aave_cfg = cfg["evm"]["aave"]

    min_tvl = float(aave_cfg.get("minTvlUsd", 10_000_000))
    markets_cfg = aave_cfg["markets"]

    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        all_rows = []

        for m in markets_cfg:
            variables = {
                "request": {
                    "chainId": int(m["chainId"]),
                    "address": m["market"]
                }
            }

            async with session.post(
                AAVE_URL,
                json={"query": AAVE_MARKET_QUERY, "variables": variables},
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json()

            market_data = payload["data"]["market"]
            chain_id = int(market_data["chain"]["chainId"])
            chain_name = market_data["chain"]["name"]

            for r in market_data["reserves"]:
                supply_apy = float(r["supplyInfo"]["apy"]["value"])
                borrow_apy = float(r["borrowInfo"]["apy"]["value"])
                util = float(r["borrowInfo"]["utilizationRate"]["value"])

                supply_total = float(r["supplyInfo"]["total"]["value"])
                usd_per_token = float(r["borrowInfo"]["total"]["usdPerToken"])
                tvl_usd = supply_total * usd_per_token

                if tvl_usd < min_tvl:
                    continue

                symbol = r["underlyingToken"]["symbol"]

                all_rows.append({
                    "segment": "evm",
                    "protocol": "aave",
                    "chain": chain_name.lower(),
                    "chainId": chain_id,
                    "symbol": symbol,
                    "supplyApy": supply_apy,
                    "borrowApy": borrow_apy,
                    "utilization": util,
                    "tvlUsd": tvl_usd,
                    "isStable": is_stable(symbol),
                    "isEthereum": is_eth(chain_id),
                })

    all_rows = sort_markets(all_rows)

    out = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "markets": all_rows
    }

    LATEST_PATH.write_text(json.dumps(out, indent=2))
    print(f"Wrote {len(all_rows)} markets")


if __name__ == "__main__":
    asyncio.run(main())
