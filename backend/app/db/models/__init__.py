# backend/app/db/models/__init__.py

# Import Base first
from app.db.session import Base

# Import existing core models
from app.db.models.user import User
from app.db.models.exchange import Exchange
from app.db.models.security import Security
from app.db.models.derivatives import Future

# Import strategy-related models
from app.db.models.strategy import Strategy
from app.db.models.strategy_security import StrategySecurity
from app.db.models.signal import Signal

# Import backtesting models
from app.db.models.backtest_run import BacktestRun
from app.db.models.backtest_result import BacktestResult
from app.db.models.backtest_trade import BacktestTrade

# Import ML models
from app.db.models.ml_model import MLModel
from app.db.models.ml_prediction import MLPrediction

# Import portfolio models
from app.db.models.portfolio import Portfolio
from app.db.models.position import Position

# Import unified pipeline models
from app.db.models.unified_ohlcv import (OHLCVUnified, DataContinuity, PipelineJob, DataQualityMetric, Timeframe, JobStatus, JobType)

# For convenient imports
__all__ = [
    'Base',
    'User',
    'Exchange',
    'Security',
    'Future',
    'Strategy',
    'StrategySecurity',
    'Signal',
    'BacktestRun',
    'BacktestResult',
    'BacktestTrade',
    'MLModel',
    'MLPrediction',
    'Portfolio',
    'Position',

    # Unified pipeline models
    'OHLCVUnified',
    'DataContinuity',
    'PipelineJob',
    'DataQualityMetric',
    'Timeframe',
    'JobStatus',
    'JobType'
]
