import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

AAVE_URL = "https://api.v3.aave.com/graphql"

AAVE_QUERY = """
query GetReserve($request: ReserveRequest!) {
  reserve(request: $request) {
    supplyInfo { apy { value } total { value } }
    borrowInfo {
      apy { value }
      total { usdPerToken }
      utilizationRate { value }
    }
  }
}
"""

ROOT = Path(__file__).resolve().parents[1]
MARKETS_PATH = ROOT / "data" / "markets.json"
LATEST_PATH = ROOT / "data" / "latest.json"


async def fetch_aave(session: aiohttp.ClientSession, item: dict) -> dict:
    variables = {
        "request": {
            "chainId": int(item["chainId"]),
            "market": item["market"],
            "underlyingToken": item["underlyingToken"],
        }
    }

    async with session.post(AAVE_URL, json={"query": AAVE_QUERY, "variables": variables}) as resp:
        resp.raise_for_status()
        payload = await resp.json()

    r = payload["data"]["reserve"]

    supply_apy = float(r["supplyInfo"]["apy"]["value"])
    borrow_apy = float(r["borrowInfo"]["apy"]["value"])
    util = float(r["borrowInfo"]["utilizationRate"]["value"])

    # "TVL" approximation for lending markets: total supplied * usdPerToken
    supply_total = float(r["supplyInfo"]["total"]["value"])
    usd_per_token = float(r["borrowInfo"]["total"]["usdPerToken"])
    tvl_usd = supply_total * usd_per_token

    return {
        "segment": "evm",
        "protocol": "aave",
        "chain": item.get("chain", "unknown"),
        "chainId": int(item["chainId"]),
        "market": item["market"],
        "symbol": item["symbol"],
        "underlyingToken": item["underlyingToken"],
        "supplyApy": supply_apy,
        "borrowApy": borrow_apy,
        "utilization": util,
        "tvlUsd": tvl_usd,
    }


async def main():
    markets_cfg = json.loads(MARKETS_PATH.read_text())

    # For now: only EVM/Aave section (you’ll add Morpho etc later)
    aave_items = markets_cfg.get("evm", {}).get("aave", [])
    if not aave_items:
        raise SystemExit("No markets found at data/markets.json -> evm -> aave")

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        results = await asyncio.gather(*[fetch_aave(session, it) for it in aave_items])

    out = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "markets": results,
    }

    LATEST_PATH.write_text(json.dumps(out, indent=2) + "\n")
    print(f"Wrote {LATEST_PATH} with {len(results)} markets")


if __name__ == "__main__":
    asyncio.run(main())
