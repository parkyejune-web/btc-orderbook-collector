"""
collectors/orderbook_collector.py
바이낸스 오더북 + 체결 스트림 수집기

수집 신호 (레짐 감지기 재료):
  1. spread          — best_ask - best_bid (절대값)
  2. spread_pct      — spread / mid_price (%)
  3. imbalance       — (bid_vol - ask_vol) / (bid_vol + ask_vol), [-1, 1]
  4. queue_depth_bid — 1호가 bid 잔량
  5. queue_depth_ask — 1호가 ask 잔량
  6. trades_per_sec  — 직전 1초 체결 건수 (Trade Arrival Rate)
  7. trade_vol_per_sec — 직전 1초 체결 수량

저장: data/orderbook/BTCUSDT_YYYYMMDD.parquet (5분마다 flush)

실행:
  python collectors/orderbook_collector.py
  python collectors/orderbook_collector.py --symbol ETHUSDT --depth 10
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUT_DIR = Path(os.environ.get("ORDERBOOK_DATA_DIR", str(Path(__file__).parent.parent / "data" / "orderbook")))
OUT_DIR.mkdir(parents=True, exist_ok=True)

FLUSH_EVERY = 300   # 5분 = 300초마다 parquet 저장
WS_BASE     = "wss://stream.binance.com:9443/ws"


class OrderbookCollector:
    def __init__(self, symbol: str = "BTCUSDT", depth_levels: int = 20):
        self.symbol      = symbol.upper()
        self.sym_lower   = symbol.lower()
        self.depth       = depth_levels

        self._rows: list[dict] = []
        self._trade_buf: deque[tuple[float, float]] = deque()  # (ts, qty)
        self._running = True
        self._last_flush = asyncio.get_event_loop().time() if False else None

    # ── 오더북 스냅샷 파싱 ──────────────────────────────────────────
    def _parse_depth(self, data: dict) -> dict | None:
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return None

        best_bid_p = float(bids[0][0])
        best_ask_p = float(asks[0][0])
        bid_vol    = sum(float(b[1]) for b in bids)
        ask_vol    = sum(float(a[1]) for a in asks)
        mid        = (best_bid_p + best_ask_p) / 2

        spread     = best_ask_p - best_bid_p
        imbalance  = (bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else 0.0

        return {
            "ts":              datetime.now(timezone.utc),
            "best_bid":        best_bid_p,
            "best_ask":        best_ask_p,
            "mid":             mid,
            "spread":          spread,
            "spread_pct":      spread / mid * 100 if mid > 0 else 0.0,
            "imbalance":       imbalance,
            "queue_depth_bid": float(bids[0][1]),
            "queue_depth_ask": float(asks[0][1]),
            "bid_vol_total":   bid_vol,
            "ask_vol_total":   ask_vol,
        }

    # ── 체결 버퍼 관리 ─────────────────────────────────────────────
    def _add_trade(self, ts: float, qty: float) -> None:
        self._trade_buf.append((ts, qty))
        cutoff = ts - 1.0
        while self._trade_buf and self._trade_buf[0][0] < cutoff:
            self._trade_buf.popleft()

    def _trade_stats(self) -> tuple[int, float]:
        return len(self._trade_buf), sum(q for _, q in self._trade_buf)

    # ── parquet flush ──────────────────────────────────────────────
    def _flush(self) -> None:
        if not self._rows:
            return
        df   = pd.DataFrame(self._rows)
        date = datetime.now(timezone.utc).strftime("%Y%m%d")
        path = OUT_DIR / f"{self.symbol}_{date}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(path, index=False)
        log.info("flush → %s  (%d rows total)", path.name, len(df))
        self._rows.clear()

    # ── WebSocket 핸들러 ───────────────────────────────────────────
    async def _depth_stream(self) -> None:
        url = f"{WS_BASE}/{self.sym_lower}@depth{self.depth}@1000ms"
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log.info("오더북 연결: %s", url)
                    async for raw in ws:
                        if not self._running:
                            break
                        data = json.loads(raw)
                        row  = self._parse_depth(data)
                        if row:
                            cnt, vol = self._trade_stats()
                            row["trades_per_sec"]   = cnt
                            row["trade_vol_per_sec"] = vol
                            self._rows.append(row)
            except Exception as e:
                if self._running:
                    log.warning("오더북 재연결 (5s): %s", e)
                    await asyncio.sleep(5)

    async def _trade_stream(self) -> None:
        url = f"{WS_BASE}/{self.sym_lower}@aggTrade"
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log.info("체결 연결: %s", url)
                    async for raw in ws:
                        if not self._running:
                            break
                        t = json.loads(raw)
                        self._add_trade(t["T"] / 1000, float(t["q"]))
            except Exception as e:
                if self._running:
                    log.warning("체결 재연결 (5s): %s", e)
                    await asyncio.sleep(5)

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(FLUSH_EVERY)
            self._flush()

    # ── 실행 진입점 ────────────────────────────────────────────────
    async def run(self) -> None:
        log.info("수집 시작: %s  depth=%d  flush=%ds", self.symbol, self.depth, FLUSH_EVERY)
        tasks = [
            asyncio.create_task(self._depth_stream()),
            asyncio.create_task(self._trade_stream()),
            asyncio.create_task(self._flush_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            for t in tasks:
                t.cancel()
            self._flush()
            log.info("수집 종료. 저장 완료.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--depth",  type=int, default=20,
                        help="오더북 레벨 수 (5/10/20)")
    args = parser.parse_args()

    collector = OrderbookCollector(args.symbol, args.depth)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _stop(*_):
        log.info("종료 신호 수신...")
        collector._running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows는 SIGTERM add_signal_handler 미지원 → Ctrl+C 만 사용
            signal.signal(sig, _stop)

    try:
        loop.run_until_complete(collector.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
