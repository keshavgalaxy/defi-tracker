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

# Minimal stable classification (DAI removed)
STABLE_SYMBOLS = {
    "USDC", "USDT", "USDE", "SUSDE", "PYUSD", "RLUSD", "USDG",
    # add more later if needed
}

def is_stable(symbol: str) -> bool:
    if not symbol:
        return False
    s = symbol.upper().strip()

    # common wrappers you may encounter
    if s.startswith("A") and s[1:] in STABLE_SYMBOLS:  # aUSDC etc
        s = s[1:]
    if s.startswith("W") and s[1:] in STABLE_SYMBOLS:  # wUSDC etc (rare)
        s = s[1:]

    return s in STABLE_SYMBOLS

def is_ethereum_chain(chain: str, chain_id: int) -> bool:
    # use both so you're robust to how you label "chain" in markets.json
    if chain_id == 1:
        return True
    c = (chain or "").lower().strip()
    return c in {"ethereum", "eth", "mainnet"}


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
    if r is None:
        raise RuntimeError(
            f"Aave reserve returned null for symbol={item.get('symbol')} chainId={item.get('chainId')}"
        )

    supply_apy = float(r["supplyInfo"]["apy"]["value"])
    borrow_apy = float(r["borrowInfo"]["apy"]["value"])
    util = float(r["borrowInfo"]["utilizationRate"]["value"])

    supply_total = float(r["supplyInfo"]["total"]["value"])
    usd_per_token = float(r["borrowInfo"]["total"]["usdPerToken"])
    tvl_usd = supply_total * usd_per_token

    sym = item["symbol"]
    chain = item.get("chain", "unknown")
    chain_id = int(item["chainId"])

    return {
        "segment": "evm",
        "protocol": "aave",
        "chain": chain,
        "chainId": chain_id,
        "market": item["market"],
        "symbol": sym,
        "underlyingToken": item["underlyingToken"],
        "supplyApy": supply_apy,
        "borrowApy": borrow_apy,
        "utilization": util,
        "tvlUsd": tvl_usd,
        "isStable": is_stable(sym),
        "isEthereum": is_ethereum_chain(chain, chain_id),
    }


def sort_markets(rows: list[dict]) -> list[dict]:
    """
    Order:
      1) stables + ethereum, borrow low->high
      2) stables + non-ethereum, borrow low->high
      3) non-stables + ethereum, supply high->low
      4) non-stables + non-ethereum, supply high->low
    """

    def key(r: dict):
        stable = bool(r.get("isStable"))
        eth = bool(r.get("isEthereum"))

        # group rank
        if stable and eth:
            group = 0
        elif stable and not eth:
            group = 1
        elif (not stable) and eth:
            group = 2
        else:
            group = 3

        borrow = float(r.get("borrowApy") or 0.0)
        supply = float(r.get("supplyApy") or 0.0)
        tvl = float(r.get("tvlUsd") or 0.0)

        # within-group sort
        if group in (0, 1):
            # stables: low borrow first
            primary = borrow
        else:
            # non-stables: high supply first
            primary = -supply

        # tie-breaker: bigger markets first
        return (group, primary, -tvl, (r.get("symbol") or ""), (r.get("chain") or ""))

    return sorted(rows, key=key)


async def main():
    markets_cfg = json.loads(MARKETS_PATH.read_text())

    aave_items = markets_cfg.get("evm", {}).get("aave", [])
    if not aave_items:
        raise SystemExit("No markets found at data/markets.json -> evm -> aave")

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        results = await asyncio.gather(*[fetch_aave(session, it) for it in aave_items])

    results = sort_markets(results)

    out = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "markets": results,
        "counts": {
            "total": len(results),
            "stable": sum(1 for r in results if r.get("isStable")),
            "nonStable": sum(1 for r in results if not r.get("isStable")),
            "ethereum": sum(1 for r in results if r.get("isEthereum")),
            "nonEthereum": sum(1 for r in results if not r.get("isEthereum")),
        },
    }

    LATEST_PATH.write_text(json.dumps(out, indent=2) + "\n")
    print(f"Wrote {LATEST_PATH} with {len(results)} markets")


if __name__ == "__main__":
    asyncio.run(main())
