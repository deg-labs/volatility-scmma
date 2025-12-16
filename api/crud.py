from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any

def get_symbols_exceeding_threshold(db: Session, timeframe: str, price_threshold: float, offset: int, direction: str, sort: str, limit: int):
    """
    指定されたタイムフレームと閾値に基づいて、価格変動率が大きい銘柄のリストを取得します。
    offsetを使用して、何本前の足と比較するかを指定できます。
    """
    table_name = f"ohlcv_{timeframe}"

    # ソート順をSQLのORDER BY句に変換
    sort_map = {
        "volatility_desc": "volatility_pct DESC",
        "volatility_asc": "volatility_pct ASC",
        "symbol_asc": "lc.symbol ASC",
    }
    order_by_clause = sort_map.get(sort, "volatility_pct DESC")

    # SQLクエリを構築
    # WITH句を使って、各シンボルごとに最新の足と、指定されたoffset前の足を取得する
    query = text(f"""
        WITH ranked_candles AS (
            -- 各シンボルについて、タイムスタンプの降順で連番を振る
            SELECT
                symbol,
                timestamp,
                close,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
            FROM {table_name}
        ),
        latest_candles AS (
            -- 最新の足 (rn=1)
            SELECT
                symbol,
                timestamp,
                close
            FROM ranked_candles
            WHERE rn = 1
        ),
        previous_candles AS (
            -- N本前の足 (rn=1+offset)
            SELECT
                symbol,
                close as prev_close
            FROM ranked_candles
            WHERE rn = 1 + :offset
        )
        -- 最新の足とN本前の足を結合し、変動率を計算
        SELECT
            lc.symbol,
            lc.timestamp as candle_ts,
            lc.close,
            pc.prev_close,
            ((lc.close - pc.prev_close) / pc.prev_close) * 100 AS volatility_pct,
            :timeframe as timeframe
        FROM latest_candles lc
        INNER JOIN previous_candles pc ON lc.symbol = pc.symbol
        WHERE
            -- 閾値でのフィルタリング
            ABS(((lc.close - pc.prev_close) / pc.prev_close) * 100) >= :price_threshold
            -- 変動方向でのフィルタリング
            AND CASE
                WHEN :direction = 'up' THEN ((lc.close - pc.prev_close) / pc.prev_close) > 0
                WHEN :direction = 'down' THEN ((lc.close - pc.prev_close) / pc.prev_close) < 0
                ELSE TRUE
            END
        ORDER BY {order_by_clause}
        LIMIT :limit
    """)

    # クエリを実行
    result = db.execute(
        query,
        {
            "price_threshold": price_threshold,
            "offset": offset,
            "direction": direction,
            "limit": limit,
            "timeframe": timeframe
        }
    )
    return result.fetchall()

from datetime import datetime, timedelta

def _parse_period_to_seconds(period_str: str) -> int:
    """Parses a period string like '24h' or '7d' into seconds."""
    unit = period_str[-1].lower()
    value = int(period_str[:-1])

    if unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 3600 * 24
    elif unit == 'w':
        return value * 3600 * 24 * 7
    # Add more units if needed (e.g., 'min' for minutes, 's' for seconds)
    raise ValueError(f"Unsupported period unit: {period_str}")

def get_volume_for_period(db: Session, timeframe: str, period_str: str, sort: str, limit: int, min_volume: float = 0) -> List[Any]:
    """
    指定された期間とタイムフレームに基づいて、各銘柄の合計出来高を取得します。
    """
    table_name = f"ohlcv_{timeframe}"

    # Convert period string to seconds, then to milliseconds for timestamp comparison
    period_seconds = _parse_period_to_seconds(period_str)
    end_ts = datetime.utcnow()
    start_ts = end_ts - timedelta(seconds=period_seconds)
    start_ts_ms = int(start_ts.timestamp() * 1000)

    # Sort order mapping
    sort_map = {
        "volume_desc": "total_volume DESC",
        "volume_asc": "total_volume ASC",
        "symbol_asc": "symbol ASC",
    }
    order_by_clause = sort_map.get(sort, "total_volume DESC")

    query = text(f"""
        SELECT
            symbol,
            SUM(volume) as total_volume
        FROM {table_name}
        WHERE
            timestamp >= :start_ts_ms
        GROUP BY
            symbol
        HAVING
            SUM(volume) > :min_volume
        ORDER BY
            {order_by_clause}
        LIMIT :limit
    """)

    result = db.execute(
        query,
        {
            "start_ts_ms": start_ts_ms,
            "limit": limit,
            "min_volume": min_volume
        }
    )
    return result.fetchall()
