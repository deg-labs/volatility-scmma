import asyncio
import time
import logging
from datetime import datetime, timedelta

import aiohttp

from client import BybitClient
from repository import DatabaseRepository
from config import AppConfig, TIMEFRAME_MAP

class DataFetchService:
    def __init__(self, client: BybitClient, repository: DatabaseRepository, config: AppConfig, logger: logging.Logger):
        self.client = client
        self.repository = repository
        self.config = config
        self.logger = logger
        self.target_symbols_cache = []
        self.target_symbols_timestamp = None
        self.cache_duration = timedelta(days=1)

    async def fetch_and_store_data(self):
        start_time = time.time()
        self.logger.info("====== 新しいデータ取得サイクルを開始 ======")

        async with aiohttp.ClientSession(timeout=self.client.timeout) as session:
            # 1. Update Target Cache if needed (Older than 24h or empty)
            now = datetime.now()
            if not self.target_symbols_cache or not self.target_symbols_timestamp or (now - self.target_symbols_timestamp) > self.cache_duration:
                self.logger.info("ターゲット銘柄（出来高上位30）を選定・更新します...")
                tickers = await self.client.get_linear_tickers(session)
                
                if not tickers:
                    self.logger.error("Ticker情報の取得に失敗したため、キャッシュ更新をスキップします。")
                    # If we have no cache at all, we can't proceed
                    if not self.target_symbols_cache:
                        self.logger.error("利用可能なキャッシュがなく、処理を中断します。")
                        return
                else:
                    # Sort by turnover24h (descending) and take top 30
                    try:
                        tickers.sort(key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
                        top_tickers = tickers[:30]
                        self.target_symbols_cache = [t["symbol"] for t in top_tickers]
                        self.target_symbols_timestamp = now
                        
                        log_msg = "【選定銘柄と24時間変動率】\n"
                        for t in top_tickers:
                            log_msg += f"{t['symbol']}: Vol={float(t.get('turnover24h', 0)):.0f}, Change={t.get('price24hPcnt')}\n"
                        self.logger.info(log_msg)
                        
                    except Exception as e:
                        self.logger.error(f"Ticker情報のソート/解析中にエラー: {e}")
                        if not self.target_symbols_cache:
                            return

            symbols = self.target_symbols_cache
            if not symbols:
                self.logger.error("対象銘柄がありません。")
                return

            self.logger.info(f"対象タイムフレーム: {self.config.timeframes}")

            for timeframe_str in self.config.timeframes:
                timeframe_str = timeframe_str.strip()
                if not timeframe_str: continue

                interval = TIMEFRAME_MAP.get(timeframe_str)
                if not interval:
                    self.logger.warning(f"未対応のタイムフレーム: {timeframe_str}。スキップします。")
                    continue

                self.logger.info(f"--- タイムフレーム: {timeframe_str} ({interval}) のデータ取得を開始 (対象: {len(symbols)}銘柄) ---")

                sem = asyncio.Semaphore(self.config.concurrency_limit)

                async def fetch_one(symbol: str):
                    async with sem:
                        return await self.client.get_kline_data(session, symbol, interval, limit=self.config.ohlcv_history_limit)

                tasks = [fetch_one(symbol) for symbol in symbols]
                results = await asyncio.gather(*tasks)

                records_to_upsert = []
                for symbol, ohlcv_data in zip(symbols, results):
                    if ohlcv_data:
                        for row in ohlcv_data:
                            records_to_upsert.append((
                                symbol, row[0], row[1], row[2], row[3], row[4], row[5], row[6]
                            ))

                if records_to_upsert:
                    self.repository.upsert_ohlcv_data(timeframe_str, records_to_upsert)

                    upserted_symbols = {rec[0] for rec in records_to_upsert}
                    self.repository.cleanup_old_ohlcv_data(timeframe_str, upserted_symbols, self.config.ohlcv_history_limit)

                self.logger.info(f"--- タイムフレーム: {timeframe_str} のデータ取得が完了 ---")

        end_time = time.time()
        self.logger.info(f"====== データ取得サイクル完了 (所要時間: {end_time - start_time:.2f}秒) ======")