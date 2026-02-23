import os
import requests
from datetime import datetime, timezone

AAVE_URL = "https://api.v3.aave.com/graphql"
MARKET = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
CHAIN_ID = 1

TOKENS = {
  "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
  "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
  "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
}

QUERY = """
query GetReserve($request: ReserveRequest!) {
  reserve(request: $request) {
    supplyInfo { apy { value } total { value } }
    borrowInfo {
      apy { value }
      total { amount { value } usd usdPerToken }
      utilizationRate { value }
    }
  }
}
"""

def fetch_one(symbol: str, underlying: str):
    variables = {"request": {"chainId": CHAIN_ID, "market": MARKET, "underlyingToken": underlying}}
    r = requests.post(AAVE_URL, json={"query": QUERY, "variables": variables}, timeout=30)
    r.raise_for_status()
    j = r.json()["data"]["reserve"]

    supply_apy = float(j["supplyInfo"]["apy"]["value"])
    borrow_apy = float(j["borrowInfo"]["apy"]["value"])
    supply_total = float(j["supplyInfo"]["total"]["value"])
    usd_per = float(j["borrowInfo"]["total"]["usdPerToken"])
    tvl_usd = supply_total * usd_per
    util = float(j["borrowInfo"]["utilizationRate"]["value"])

    return {
      "ts": datetime.now(timezone.utc).isoformat(),
      "chain_id": CHAIN_ID,
      "market": MARKET,
      "symbol": symbol,
      "supply_apy": supply_apy,
      "borrow_apy": borrow_apy,
      "tvl_usd": tvl_usd,
      "utilization": util
    }

def main():
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_KEY"]

    rows = [fetch_one(sym, addr) for sym, addr in TOKENS.items()]

    r = requests.post(
        f"{supabase_url}/rest/v1/rate_snapshots",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=rows,
        timeout=30,
    )

    r.raise_for_status()
    print("Inserted", len(rows), "rows")

if __name__ == "__main__":
    main()
