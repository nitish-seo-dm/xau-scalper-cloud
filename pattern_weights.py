from supabase import create_client
import pandas as pd
import os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"),
                         os.getenv("SUPABASE_ANON_KEY"))


def compute_pattern_weights():
    print("⏳ Fetching trades from Supabase...")
    result = supabase.table("trades").select("*").limit(100000).execute()
    rows = result.data
    if not rows:
        print("❌ No data to process.")
        return

    df = pd.DataFrame(rows)
    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0)

    candlestick_memory = defaultdict(list)
    chart_memory = defaultdict(list)

    for _, row in df.iterrows():
        for pattern in str(row.get("candlestick_patterns", "")).split("|"):
            if pattern.strip() and pattern.strip() != "None":
                candlestick_memory[pattern.strip()].append(row["pnl"])
        for pattern in str(row.get("chart_patterns", "")).split("|"):
            if pattern.strip() and pattern.strip() != "None":
                chart_memory[pattern.strip()].append(row["pnl"])

    def build_payload(memory):
        return [{
            "pattern": k,
            "mean_pnl": round(sum(v) / len(v), 4),
            "count": len(v)
        } for k, v in memory.items()]

    print("✅ Saving pattern memory to Supabase...")

    supabase.table("pattern_weights_candlestick").upsert(
        build_payload(candlestick_memory)).execute()
    supabase.table("pattern_weights_chart").upsert(
        build_payload(chart_memory)).execute()

    print("✅ Pattern memory updated.")


if __name__ == "__main__":
    compute_pattern_weights()
