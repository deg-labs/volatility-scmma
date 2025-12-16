import os
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.orm import Session
from typing import List
from enum import Enum

import crud
import schemas
from database import engine, get_db

app = FastAPI(
    title="CMMA API",
    description="BybitのOHLCVデータから価格変動率を計算するAPI",
    version="2.0.0",
)

# --- エラーハンドリング ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=schemas.ErrorResponse(
            error=schemas.ErrorDetail(
                code=exc.headers.get("X-Error-Code", "HTTP_EXCEPTION"),
                message=exc.detail
            )
        ).model_dump(),
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # バリデーションエラーのメッセージを整形
    error_messages = []
    for error in exc.errors():
        field = "->".join(map(str, error['loc']))
        message = error['msg']
        error_messages.append(f"[{field}]: {message}")
    
    return JSONResponse(
        status_code=422,
        content=schemas.ErrorResponse(
            error=schemas.ErrorDetail(
                code="INVALID_INPUT",
                message=", ".join(error_messages)
            )
        ).model_dump(),
    )

# --- パラメータ用Enum ---
class Direction(str, Enum):
    up = "up"
    down = "down"
    both = "both"

class SortBy(str, Enum):
    volatility_desc = "volatility_desc"
    volatility_asc = "volatility_asc"
    symbol_asc = "symbol_asc"

VALID_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]

# --- エンドポイント ---
@app.get(
    "/volatility", 
    response_model=schemas.VolatilityResponse,
    summary="価格変動率の高い銘柄を取得",
    response_description="条件に一致した銘柄の変動率データ"
)
def read_volatility(
    timeframe: str = Query(..., description=f"タイムフレームを指定。有効値: {', '.join(VALID_TIMEFRAMES)}"),
    price_threshold: float = Query(..., gt=0, description="価格変動率の閾値(%)。絶対値で比較されます。例: 5.0", alias="threshold"),
    offset: int = Query(1, gt=0, description="何本前のローソク足と比較するか。デフォルトは1 (1本前)。"),
    direction: Direction = Query(Direction.both, description="変動方向をフィルタ"),
    sort: SortBy = Query(SortBy.volatility_desc, description="結果のソート順"),
    limit: int = Query(100, gt=0, le=500, description="取得する最大件数"),
    db: Session = Depends(get_db)
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"無効なタイムフレームです。有効な値: {', '.join(VALID_TIMEFRAMES)}",
            headers={"X-Error-Code": "INVALID_TIMEFRAME"},
        )
    
    results = crud.get_symbols_exceeding_threshold(
        db=db, 
        timeframe=timeframe, 
        price_threshold=price_threshold,
        offset=offset,
        direction=direction.value,
        sort=sort.value,
        limit=limit
    )
    
    # crudからの結果をレスポンスモデルに変換
    volatility_data = [
        schemas.VolatilityData(
            symbol=row.symbol,
            timeframe=row.timeframe,
            candle_ts=row.candle_ts,
            price=schemas.PriceInfo(
                close=row.close,
                prev_close=row.prev_close
            ),
            change=schemas.ChangeInfo(
                pct=round(row.volatility_pct, 4),
                direction="up" if row.volatility_pct > 0 else "down"
            )
        ) for row in results
    ]

    return schemas.VolatilityResponse(count=len(volatility_data), data=volatility_data)

@app.get("/", include_in_schema=False)
def read_root():
    return {"message": "Welcome to CMMA API v2. See /docs for details."}


# Read OHLCV_HISTORY_LIMIT from environment
# Default to 5 if not set, matching the original .env.example
OHLCV_HISTORY_LIMIT = int(os.getenv("OHLCV_HISTORY_LIMIT", "5"))

# --- パラメータ用Enum ---
class Direction(str, Enum):
    up = "up"
    down = "down"
    both = "both"

class SortBy(str, Enum):
    volatility_desc = "volatility_desc"
    volatility_asc = "volatility_asc"
    symbol_asc = "symbol_asc"

class VolumeSortBy(str, Enum):
    volume_desc = "volume_desc"
    volume_asc = "volume_asc"
    symbol_asc = "symbol_asc"

VALID_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]
VALID_PERIODS = ["1h", "6h", "12h", "24h", "1d", "7d", "1w", "1M"] # Add more as needed, consistent with _parse_period_to_seconds

# Helper function to convert timeframe string to minutes
def _parse_timeframe_to_minutes(timeframe_str: str) -> int:
    unit = timeframe_str[-1].lower()
    value = int(timeframe_str[:-1])
    if unit == 'm':
        return value
    elif unit == 'h':
        return value * 60
    elif unit == 'd':
        return value * 60 * 24
    elif unit == 'w':
        return value * 60 * 24 * 7
    elif unit == 'M': # Assuming 'M' is for month, roughly 30 days
        return value * 60 * 24 * 30
    raise ValueError(f"Unsupported timeframe unit: {timeframe_str}")

# Helper function to convert period string to minutes (reusing from crud, but needs to be accessible here for validation)
# This is a bit of duplication, but necessary for validation before CRUD call.
def _parse_period_to_minutes(period_str: str) -> int:
    unit = period_str[-1].lower()
    value = int(period_str[:-1])

    if unit == 'h':
        return value * 60
    elif unit == 'd':
        return value * 60 * 24
    elif unit == 'w':
        return value * 60 * 24 * 7
    elif unit == 'm' and len(period_str) > 1 and period_str[:-1].isdigit(): # Check if it's 'min' not 'month' for period
        return value
    raise ValueError(f"Unsupported period unit: {period_str}")


# --- エンドポイント ---
@app.get(
    "/volatility", 
    response_model=schemas.VolatilityResponse,
    summary="価格変動率の高い銘柄を取得",
    response_description="条件に一致した銘柄の変動率データ"
)
def read_volatility(
    timeframe: str = Query(..., description=f"タイムフレームを指定。有効値: {', '.join(VALID_TIMEFRAMES)}"),
    price_threshold: float = Query(..., gt=0, description="価格変動率の閾値(%)。絶対値で比較されます。例: 5.0", alias="threshold"),
    offset: int = Query(1, gt=0, description="何本前のローソク足と比較するか。デフォルトは1 (1本前)。"),
    direction: Direction = Query(Direction.both, description="変動方向をフィルタ"),
    sort: SortBy = Query(SortBy.volatility_desc, description="結果のソート順"),
    limit: int = Query(100, gt=0, le=500, description="取得する最大件数"),
    db: Session = Depends(get_db)
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"無効なタイムフレームです。有効な値: {', '.join(VALID_TIMEFRAMES)}",
            headers={"X-Error-Code": "INVALID_TIMEFRAME"},
        )
    
    results = crud.get_symbols_exceeding_threshold(
        db=db, 
        timeframe=timeframe, 
        price_threshold=price_threshold,
        offset=offset,
        direction=direction.value,
        sort=sort.value,
        limit=limit
    )
    
    # crudからの結果をレスポンスモデルに変換
    volatility_data = [
        schemas.VolatilityData(
            symbol=row.symbol,
            timeframe=row.timeframe,
            candle_ts=row.candle_ts,
            price=schemas.PriceInfo(
                close=row.close,
                prev_close=row.prev_close
            ),
            change=schemas.ChangeInfo(
                pct=round(row.volatility_pct, 4),
                direction="up" if row.volatility_pct > 0 else "down"
            )
        ) for row in results
    ]

    return schemas.VolatilityResponse(count=len(volatility_data), data=volatility_data)

@app.get(
    "/volume",
    response_model=schemas.VolumeResponse,
    summary="指定期間の出来高ランキングを取得",
    response_description="条件に一致した銘柄の合計出来高データ"
)
def read_volume(
    timeframe: str = Query(..., description=f"出来高集計に使うOHLCVのタイムフレーム。有効値: {', '.join(VALID_TIMEFRAMES)}"),
    period: str = Query(..., description=f"出来高を集計する期間 (例: '24h', '7d')。有効値: {', '.join(VALID_PERIODS)}"),
    min_volume: float = Query(None, gt=0, description="期間内の合計出来高での足切り(USD)。例: 500000000 (500M USD)"),
    sort: VolumeSortBy = Query(VolumeSortBy.volume_desc, description="結果のソート順"),
    limit: int = Query(100, gt=0, le=500, description="取得する最大件数"),
    db: Session = Depends(get_db)
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"無効なタイムフレームです。有効な値: {', '.join(VALID_TIMEFRAMES)}",
            headers={"X-Error-Code": "INVALID_TIMEFRAME"},
        )
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"無効な期間指定です。有効な値: {', '.join(VALID_PERIODS)}",
            headers={"X-Error-Code": "INVALID_PERIOD"},
        )

    try:
        timeframe_minutes = _parse_timeframe_to_minutes(timeframe)
        period_minutes = _parse_period_to_minutes(period)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e), headers={"X-Error-Code": "INVALID_UNIT"})

    if timeframe_minutes == 0: # Should not happen with current _parse_timeframe_to_minutes, but good for safety
        raise HTTPException(status_code=400, detail="Timeframe cannot be zero minutes.", headers={"X-Error-Code": "INVALID_TIMEFRAME"})

    # Calculate required candles and check against OHLCV_HISTORY_LIMIT
    required_candles = period_minutes // timeframe_minutes # Use integer division
    
    if required_candles > OHLCV_HISTORY_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"指定された期間 ({period}) とタイムフレーム ({timeframe}) の組み合わせでは、"
                   f"{required_candles}本のローソク足が必要です。これは現在利用可能な履歴の最大本数"
                   f"({OHLCV_HISTORY_LIMIT}本) を超えています。より短い期間、またはより大きな"
                   f"タイムフレームを選択してください。",
            headers={"X-Error-Code": "INSUFFICIENT_HISTORY"}
        )

    results = crud.get_volume_for_period(
        db=db,
        timeframe=timeframe,
        period_str=period,
        sort=sort.value,
        limit=limit,
        min_volume=min_volume or 0
    )

    volume_data = [
        schemas.VolumeData(
            symbol=row.symbol,
            total_volume=round(row.total_volume, 4),
            timeframe=timeframe,
            period=period
        ) for row in results
    ]

    return schemas.VolumeResponse(count=len(volume_data), data=volume_data)
