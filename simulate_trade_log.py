import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def generate_trades(batch_size=10000, total_rows=10_000_000):
    start_time = datetime.now()
    symbol = "XAUUSDm"

    for chunk_start in range(0, total_rows, batch_size):
        chunk = []
        for i in range(batch_size):
            time = start_time + timedelta(minutes=chunk_start + i)
            action = random.choice(["BUY", "SELL"])
            price = round(1900 + random.uniform(-5, 5), 2)
            retcode = 10009 if random.random() > 0.4 else 10016
            comment = "Take profit" if retcode == 10009 else "Stop loss"
            pnl = round(random.uniform(0.5, 2.5),
                        2) if retcode == 10009 else round(
                            random.uniform(-2.5, -0.5), 2)

            # Simulated patterns
            candlestick = random.choice(
                ["Hammer", "Engulfing", "Doji", "None"])
            chart = random.choice(
                ["Double Top", "Double Bottom", "Triangle", "None"])

            chunk.append({
                "timestamp": time.isoformat(),
                "price": price,
                "pnl": pnl,
                "spread": round(random.uniform(80, 200), 1),
                "tick_volume": random.randint(100, 1000),
                "signal": action,
                "candlestick_patterns": candlestick,
                "chart_patterns": chart,
                "comment": comment
            })

        # Insert into Supabase
        data = supabase.table("trades").insert(chunk).execute()
        print(f"✅ Inserted rows {chunk_start} to {chunk_start + batch_size}")


if __name__ == "__main__":
    generate_trades()
