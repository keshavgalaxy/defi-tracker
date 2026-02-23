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

# DAI removed
STABLE_SYMBOLS = {"USDC", "USDT", "USDE", "SUSDE", "PYUSD", "RLUSD", "USDG"}


def is_stable(symbol: str) -> bool:
    if not symbol:
        return False
    s = symbol.upper().strip()
    if s.startswith("A") and s[1:] in STABLE_SYMBOLS:
        s = s[1:]
    return s in STABLE_SYMBOLS


def is_eth(chain_id: int) -> bool:
    return chain_id == 1


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

        if stable and eth:
            group = 0
            primary = float(r.get("borrowApy") or 0.0)
        elif stable and not eth:
            group = 1
            primary = float(r.get("borrowApy") or 0.0)
        elif (not stable) and eth:
            group = 2
            primary = -float(r.get("supplyApy") or 0.0)
        else:
            group = 3
            primary = -float(r.get("supplyApy") or 0.0)

        tvl = -float(r.get("tvlUsd") or 0.0)
        sym = r.get("symbol") or ""
        chain = r.get("chain") or ""
        return (group, primary, tvl, sym, chain)

    return sorted(rows, key=key)


async def main():
    cfg = json.loads(MARKETS_PATH.read_text())
    aave_cfg = cfg.get("evm", {}).get("aave", {})
    if not isinstance(aave_cfg, dict):
        raise SystemExit("data/markets.json -> evm -> aave must be an object with keys: minTvlUsd, markets")

    min_tvl = float(aave_cfg.get("minTvlUsd", 10_000_000))
    markets_cfg = aave_cfg.get("markets", [])
    if not markets_cfg:
        raise SystemExit("No markets found at data/markets.json -> evm -> aave -> markets")

    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=20)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        all_rows: list[dict] = []
        totals = {"reserves": 0, "skipped_nulls": 0, "skipped_small": 0, "kept": 0}

        for m in markets_cfg:
            variables = {
                "request": {"chainId": int(m["chainId"]), "address": m["market"]}
            }

            async with session.post(AAVE_URL, json={"query": AAVE_MARKET_QUERY, "variables": variables}) as resp:
                resp.raise_for_status()
                payload = await resp.json()

            if payload.get("errors"):
                raise RuntimeError(f"GraphQL errors: {payload['errors']}")

            market_data = payload.get("data", {}).get("market")
            if not market_data:
                raise RuntimeError(f"Market not found for chainId={m['chainId']} address={m['market']}")

            chain_id = int(market_data["chain"]["chainId"])
            chain_name = (market_data["chain"]["name"] or "").lower()

            reserves = market_data.get("reserves") or []
            for r in reserves:
                totals["reserves"] += 1

                token = r.get("underlyingToken") or {}
                symbol = token.get("symbol") or ""
                underlying_addr = token.get("address") or ""

                supply_info = r.get("supplyInfo")
                borrow_info = r.get("borrowInfo")
                if not supply_info or not borrow_info:
                    totals["skipped_nulls"] += 1
                    continue

                supply_apy_obj = supply_info.get("apy") or {}
                borrow_apy_obj = borrow_info.get("apy") or {}
                util_obj = borrow_info.get("utilizationRate") or {}
                borrow_total = borrow_info.get("total") or {}
                supply_total_obj = supply_info.get("total") or {}

                # Skip if any critical numeric field is missing
                if (
                    supply_apy_obj.get("value") is None
                    or borrow_apy_obj.get("value") is None
                    or util_obj.get("value") is None
                    or borrow_total.get("usdPerToken") is None
                    or supply_total_obj.get("value") is None
                ):
                    totals["skipped_nulls"] += 1
                    continue

                supply_apy = float(supply_apy_obj["value"])
                borrow_apy = float(borrow_apy_obj["value"])
                util = float(util_obj["value"])

                supply_total = float(supply_total_obj["value"])
                usd_per_token = float(borrow_total["usdPerToken"])
                tvl_usd = supply_total * usd_per_token

                if tvl_usd < min_tvl:
                    totals["skipped_small"] += 1
                    continue

                row = {
                    "segment": "evm",
                    "protocol": "aave",
                    "chain": chain_name,
                    "chainId": chain_id,
                    "market": m["market"],
                    "symbol": symbol,
                    "underlyingToken": underlying_addr,
                    "supplyApy": supply_apy,
                    "borrowApy": borrow_apy,
                    "utilization": util,
                    "tvlUsd": tvl_usd,
                    "isStable": is_stable(symbol),
                    "isEthereum": is_eth(chain_id),
                }

                all_rows.append(row)
                totals["kept"] += 1

        all_rows = sort_markets(all_rows)

    out = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "markets": all_rows,
        "counts": totals,
        "minTvlUsd": min_tvl,
    }

    LATEST_PATH.write_text(json.dumps(out, indent=2) + "\n")
    print(f"Wrote {LATEST_PATH} with {len(all_rows)} markets. Stats: {totals}")


if __name__ == "__main__":
    asyncio.run(main())
