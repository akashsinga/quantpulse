# Import Base first
from app.db.session import Base

# Import models with no dependencies first
from app.db.models.user import User
from app.db.models.exchange import Exchange

# Import models with dependencies on the above
from app.db.models.security import Security

# Import derivative related models
from app.db.models.derivatives import Future

# Import time-series data models
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_weekly import OHLCVWeekly
from app.db.models.technical_indicators import TechnicalIndicator

# Import OHLCV progress tracking - NEW
from app.db.models.ohlcv_progress import OHLCVProgress

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

# For convenient imports
__all__ = [
    'Base',
    'User',
    'Exchange',
    'Security',
    'Future',
    'OHLCVDaily',
    'OHLCVWeekly',
    'TechnicalIndicator',
    'OHLCVProgress',  # NEW
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
]
