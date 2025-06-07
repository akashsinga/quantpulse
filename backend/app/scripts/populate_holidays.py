# backend/app/scripts/populate_holidays.py

import uuid
from datetime import date

from app.db.session import get_db
from app.db.models.exchange import Exchange
from app.db.models.market_holiday import MarketHoliday
from app.utils.logger import get_logger

logger = get_logger(__name__)

NSE_HOLIDAYS_DATA = {
    2000: [("2000-01-26", "Republic Day"), ("2000-03-21", "Holi"), ("2000-04-21", "Good Friday"), ("2000-08-15", "Independence Day"), ("2000-09-01", "Ganesh Chaturthi"), ("2000-10-02", "Gandhi Jayanti"), ("2000-10-26", "Diwali"), ("2000-11-11", "Guru Nanak Jayanti"), ("2000-12-25", "Christmas")],
    2001: [("2001-01-26", "Republic Day"), ("2001-03-09", "Holi"), ("2001-04-13", "Good Friday"), ("2001-08-15", "Independence Day"), ("2001-08-29", "Ganesh Chaturthi"), ("2001-10-02", "Gandhi Jayanti"), ("2001-11-14", "Diwali"), ("2001-11-01", "Guru Nanak Jayanti"), ("2001-12-25", "Christmas")],
    2002: [("2002-01-26", "Republic Day"), ("2002-03-29", "Holi"), ("2002-03-29", "Good Friday"), ("2002-08-15", "Independence Day"), ("2002-09-16", "Ganesh Chaturthi"), ("2002-10-02", "Gandhi Jayanti"), ("2002-11-04", "Diwali"), ("2002-11-20", "Guru Nanak Jayanti"), ("2002-12-25", "Christmas")],
    2003: [("2003-01-26", "Republic Day"), ("2003-03-18", "Holi"), ("2003-04-18", "Good Friday"), ("2003-08-15", "Independence Day"), ("2003-09-05", "Ganesh Chaturthi"), ("2003-10-02", "Gandhi Jayanti"), ("2003-10-25", "Diwali"), ("2003-11-08", "Guru Nanak Jayanti"), ("2003-12-25", "Christmas")],
    2004: [("2004-01-26", "Republic Day"), ("2004-03-07", "Holi"), ("2004-04-09", "Good Friday"), ("2004-08-15", "Independence Day"), ("2004-09-24", "Ganesh Chaturthi"), ("2004-10-02", "Gandhi Jayanti"), ("2004-11-12", "Diwali"), ("2004-11-26", "Guru Nanak Jayanti"), ("2004-12-25", "Christmas")],
    2005: [("2005-01-26", "Republic Day"), ("2005-03-26", "Holi"), ("2005-03-25", "Good Friday"), ("2005-08-15", "Independence Day"), ("2005-09-13", "Ganesh Chaturthi"), ("2005-10-02", "Gandhi Jayanti"), ("2005-11-01", "Diwali"), ("2005-11-15", "Guru Nanak Jayanti"), ("2005-12-25", "Christmas")],
    2006: [("2006-01-26", "Republic Day"), ("2006-03-15", "Holi"), ("2006-04-14", "Good Friday"), ("2006-08-15", "Independence Day"), ("2006-08-27", "Ganesh Chaturthi"), ("2006-10-02", "Gandhi Jayanti"), ("2006-10-21", "Diwali"), ("2006-11-05", "Guru Nanak Jayanti"), ("2006-12-25", "Christmas")],
    2007: [("2007-01-26", "Republic Day"), ("2007-03-04", "Holi"), ("2007-04-06", "Good Friday"), ("2007-08-15", "Independence Day"), ("2007-09-15", "Ganesh Chaturthi"), ("2007-10-02", "Gandhi Jayanti"), ("2007-11-09", "Diwali"), ("2007-11-24", "Guru Nanak Jayanti"), ("2007-12-25", "Christmas")],
    2008: [("2008-01-26", "Republic Day"), ("2008-03-22", "Holi"), ("2008-03-21", "Good Friday"), ("2008-08-15", "Independence Day"), ("2008-09-03", "Ganesh Chaturthi"), ("2008-10-02", "Gandhi Jayanti"), ("2008-10-28", "Diwali"), ("2008-11-13", "Guru Nanak Jayanti"), ("2008-12-25", "Christmas")],
    2009: [("2009-01-26", "Republic Day"), ("2009-03-11", "Holi"), ("2009-04-10", "Good Friday"), ("2009-08-15", "Independence Day"), ("2009-08-23", "Ganesh Chaturthi"), ("2009-10-02", "Gandhi Jayanti"), ("2009-10-17", "Diwali"), ("2009-11-02", "Guru Nanak Jayanti"), ("2009-12-25", "Christmas")],
    2010: [("2010-01-26", "Republic Day"), ("2010-03-01", "Holi"), ("2010-04-02", "Good Friday"), ("2010-08-15", "Independence Day"), ("2010-09-11", "Ganesh Chaturthi"), ("2010-10-02", "Gandhi Jayanti"), ("2010-11-05", "Diwali"), ("2010-11-21", "Guru Nanak Jayanti"), ("2010-12-25", "Christmas")],
    2011: [("2011-01-26", "Republic Day"), ("2011-03-20", "Holi"), ("2011-04-22", "Good Friday"), ("2011-08-15", "Independence Day"), ("2011-09-01", "Ganesh Chaturthi"), ("2011-10-02", "Gandhi Jayanti"), ("2011-10-26", "Diwali"), ("2011-11-10", "Guru Nanak Jayanti"), ("2011-12-25", "Christmas")],
    2012: [("2012-01-26", "Republic Day"), ("2012-03-08", "Holi"), ("2012-04-06", "Good Friday"), ("2012-08-15", "Independence Day"), ("2012-09-19", "Ganesh Chaturthi"), ("2012-10-02", "Gandhi Jayanti"), ("2012-11-13", "Diwali"), ("2012-11-28", "Guru Nanak Jayanti"), ("2012-12-25", "Christmas")],
    2013: [("2013-01-26", "Republic Day"), ("2013-03-27", "Holi"), ("2013-03-29", "Good Friday"), ("2013-08-15", "Independence Day"), ("2013-09-09", "Ganesh Chaturthi"), ("2013-10-02", "Gandhi Jayanti"), ("2013-11-03", "Diwali"), ("2013-11-17", "Guru Nanak Jayanti"), ("2013-12-25", "Christmas")],
    2014: [("2014-01-26", "Republic Day"), ("2014-03-17", "Holi"), ("2014-04-18", "Good Friday"), ("2014-08-15", "Independence Day"), ("2014-08-29", "Ganesh Chaturthi"), ("2014-10-02", "Gandhi Jayanti"), ("2014-10-23", "Diwali"), ("2014-11-06", "Guru Nanak Jayanti"), ("2014-12-25", "Christmas")],
    2015: [("2015-01-26", "Republic Day"), ("2015-03-06", "Holi"), ("2015-04-03", "Good Friday"), ("2015-08-15", "Independence Day"), ("2015-09-17", "Ganesh Chaturthi"), ("2015-10-02", "Gandhi Jayanti"), ("2015-11-11", "Diwali"), ("2015-11-25", "Guru Nanak Jayanti"), ("2015-12-25", "Christmas")],
    2016: [("2016-01-26", "Republic Day"), ("2016-03-24", "Holi"), ("2016-03-25", "Good Friday"), ("2016-08-15", "Independence Day"), ("2016-09-05", "Ganesh Chaturthi"), ("2016-10-02", "Gandhi Jayanti"), ("2016-10-30", "Diwali"), ("2016-11-14", "Guru Nanak Jayanti"), ("2016-12-25", "Christmas")],
    2017: [("2017-01-26", "Republic Day"), ("2017-03-13", "Holi"), ("2017-04-14", "Good Friday"), ("2017-08-15", "Independence Day"), ("2017-08-25", "Ganesh Chaturthi"), ("2017-10-02", "Gandhi Jayanti"), ("2017-10-19", "Diwali"), ("2017-11-04", "Guru Nanak Jayanti"), ("2017-12-25", "Christmas")],
    2018: [("2018-01-26", "Republic Day"), ("2018-03-02", "Holi"), ("2018-03-30", "Good Friday"), ("2018-08-15", "Independence Day"), ("2018-09-13", "Ganesh Chaturthi"), ("2018-10-02", "Gandhi Jayanti"), ("2018-11-07", "Diwali"), ("2018-11-23", "Guru Nanak Jayanti"), ("2018-12-25", "Christmas")],
    2019: [("2019-01-26", "Republic Day"), ("2019-03-21", "Holi"), ("2019-04-19", "Good Friday"), ("2019-08-15", "Independence Day"), ("2019-09-02", "Ganesh Chaturthi"), ("2019-10-02", "Gandhi Jayanti"), ("2019-10-27", "Diwali"), ("2019-11-12", "Guru Nanak Jayanti"), ("2019-12-25", "Christmas")],
    2020: [("2020-01-26", "Republic Day"), ("2020-03-10", "Holi"), ("2020-04-10", "Good Friday"), ("2020-08-15", "Independence Day"), ("2020-08-22", "Ganesh Chaturthi"), ("2020-10-02", "Gandhi Jayanti"), ("2020-11-14", "Diwali"), ("2020-11-30", "Guru Nanak Jayanti"), ("2020-12-25", "Christmas")],
    2021: [("2021-01-26", "Republic Day"), ("2021-03-29", "Holi"), ("2021-04-02", "Good Friday"), ("2021-08-15", "Independence Day"), ("2021-09-10", "Ganesh Chaturthi"), ("2021-10-02", "Gandhi Jayanti"), ("2021-11-04", "Diwali"), ("2021-11-19", "Guru Nanak Jayanti"), ("2021-12-25", "Christmas")],
    2022: [("2022-01-26", "Republic Day"), ("2022-03-18", "Holi"), ("2022-04-15", "Good Friday"), ("2022-08-15", "Independence Day"), ("2022-08-31", "Ganesh Chaturthi"), ("2022-10-02", "Gandhi Jayanti"), ("2022-10-24", "Diwali"), ("2022-11-08", "Guru Nanak Jayanti"), ("2022-12-25", "Christmas")],
    2023: [("2023-01-26", "Republic Day"), ("2023-03-08", "Holi"), ("2023-04-07", "Good Friday"), ("2023-08-15", "Independence Day"), ("2023-09-19", "Ganesh Chaturthi"), ("2023-10-02", "Gandhi Jayanti"), ("2023-11-12", "Diwali"), ("2023-11-27", "Guru Nanak Jayanti"), ("2023-12-25", "Christmas")],
    2024: [("2024-01-26", "Republic Day"), ("2024-03-25", "Holi"), ("2024-03-29", "Good Friday"), ("2024-08-15", "Independence Day"), ("2024-09-07", "Ganesh Chaturthi"), ("2024-10-02", "Gandhi Jayanti"), ("2024-11-01", "Diwali"), ("2024-11-15", "Guru Nanak Jayanti"), ("2024-12-25", "Christmas")],
    2025: [("2025-01-26", "Republic Day"), ("2025-03-14", "Holi"), ("2025-04-18", "Good Friday"), ("2025-08-15", "Independence Day"), ("2025-08-27", "Ganesh Chaturthi"), ("2025-10-02", "Gandhi Jayanti"), ("2025-10-21", "Diwali"), ("2025-11-05", "Guru Nanak Jayanti"), ("2025-12-25", "Christmas")]
}


def populate_market_holidays():
    """Populate market holidays fro NSE from 2000-2025"""
    with get_db() as db:
        # Get NSE exchange
        nse_exchange = db.query(Exchange).filter(Exchange.code == 'NSE').first()

        if not nse_exchange:
            nse_exchange = Exchange(id=uuid.UUID("984ffe13-dcfb-4362-8291-5f2bee2645ef"), name="National Stock Exchange", code="NSE", country="India", timezone="Asia/Kolkata", is_active=True)
            db.add(nse_exchange)
            db.commit()
            logger.info("Created NSE exchange")

        total_holidays = 0
        skipped_holidays = 0

        for year, holidays in NSE_HOLIDAYS_DATA.items():
            logger.info(f"Processing holidays for year {year}")

            for date_str, name in holidays:
                try:
                    holiday_date = date.fromisoformat(date_str)

                    # Check if holiday already exists
                    existing = db.query(MarketHoliday).filter(MarketHoliday.holiday_date == holiday_date, MarketHoliday.exchange_id == nse_exchange.id).first()

                    if existing:
                        logger.debug(f"Holiday already exists: {name} on {holiday_date}")
                        skipped_holidays += 1
                        continue

                    holiday = MarketHoliday(holiday_date=holiday_date, exchange_id=nse_exchange.id, holiday_name=name, holiday_type="full", description=f"{name} - NSE Trading Holiday")
                    db.add(holiday)
                    total_holidays += 1
                    logger.debug(f"Added holiday: {name} on {holiday_date}")
                except ValueError as e:
                    logger.error(f"Invalid date format {date_str}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error adding holiday {name} on {date_str}: {e}")
                    continue

        try:
            db.commit()
            logger.info(f"Successfully added {total_holidays} market holidays to database")
            logger.info(f"Skipped {skipped_holidays} existing holidays")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to commit holidays to database : {e}")
            raise


if __name__ == "__main__":
    populate_market_holidays()
