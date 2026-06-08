#!/usr/bin/env python3
"""
One-shot poller for GitHub Actions. Fetches one snapshot and exits.
Exit 0 on success, 1 on failure.
"""
import logging
import os
import sys

from busha_spread_tracker import (
    BUSHA_PROD, BUSHA_SANDBOX, DEFAULT_MARKUP_BPS, DEFAULT_KES_MARKUP_BPS,
    make_provider, make_database, init_db, init_kes_db, SpreadPoller, push_to_sheets_once,
)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    env = os.environ.get("BUSHA_ENV", "prod")
    busha_base = BUSHA_PROD if env == "prod" else BUSHA_SANDBOX
    api_key = os.environ.get("BUSHA_API_KEY")

    try:
        db = make_database()
        init_db(db)
        init_kes_db(db)
    except SystemExit:
        raise
    except Exception as e:
        logging.error("DB init failed: %s", e)
        return 1

    pair = os.environ.get("PAIR", "USDTNGN").upper()
    markup_bps = float(os.environ.get(
        "PV_KES_MARKUP_BPS" if pair.endswith("KES") else "PV_MARKUP_BPS",
        DEFAULT_KES_MARKUP_BPS if pair.endswith("KES") else DEFAULT_MARKUP_BPS,
    ))
    provider = make_provider()
    poller = SpreadPoller(db=db, busha_base=busha_base, busha_api_key=api_key,
                          provider=provider, markup_bps=markup_bps, pair=pair)
    try:
        snap = poller.poll_once()
        logging.info("Poll complete: %s", snap)
    except Exception as e:
        logging.error("Poll failed: %s", e)
        db.close()
        return 1

    try:
        push_to_sheets_once(db)
    except Exception as e:
        logging.warning("Sheets push failed (non-fatal): %s", e)

    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
