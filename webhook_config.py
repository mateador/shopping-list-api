"""Central tunables for webhook delivery. Override via env vars if needed."""
import os

# Total tries per delivery (first attempt included).
MAX_ATTEMPTS = int(os.environ.get("WEBHOOK_MAX_ATTEMPTS", 3))

# Wait before retry n is BASE_DELAY_SECONDS * 2 ** (n - 1): 1s, then 2s.
BASE_DELAY_SECONDS = float(os.environ.get("WEBHOOK_BASE_DELAY_SECONDS", 1.0))

# Hard limit on each outbound request.
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("WEBHOOK_TIMEOUT_SECONDS", 10.0))

# Cap on active subscriptions per household.
MAX_ACTIVE_SUBSCRIPTIONS = int(os.environ.get("WEBHOOK_MAX_SUBSCRIPTIONS", 5))

MAX_URL_LENGTH = 2048
MAX_ERROR_LENGTH = 500
