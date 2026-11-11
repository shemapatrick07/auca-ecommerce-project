"""HBase helpers: connection + row-key design (shared by the HBase scripts)."""
from datetime import datetime, timezone
import happybase
import config

LONG_MAX = 2 ** 63 - 1


def connect():
    return happybase.Connection(config.HBASE_HOST, port=config.HBASE_PORT, timeout=120000)


def epoch_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def rev_ts(dt: datetime) -> str:
    """Reverse timestamp, zero padded to 19 digits so newest rows sort FIRST lexicographically."""
    return f"{LONG_MAX - epoch_ms(dt):019d}"


def session_row_key(user_id: str, start: datetime, session_id: str) -> bytes:
    # <user_id>#<reverse_ts>#<session_id>  -> all sessions of a user are contiguous, newest first
    return f"{user_id}#{rev_ts(start)}#{session_id}".encode()


def metric_row_key(product_id: str, yyyymmdd: str) -> bytes:
    # <product_id>#<yyyymmdd>  -> a product's daily metrics are contiguous and time-ordered
    return f"{product_id}#{yyyymmdd}".encode()
