import datetime
import logging
import json

import envfile
import database

from sqlalchemy import select, func

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

TOTAL_EQUIP = 99

def read_dates():
    logging.info('Reading .env')
    envfile.load()

    logging.info('Connecting to database')
    db = database.connect()
    logging.info('Connection OK')

    logging.info('Reading metadata')
    tbl_eq_file = db.tables['radars.equipment_files']

    today = datetime.date.today()
    date = datetime.date(2018, 1, 1)

    logging.info(f'Counting from {str(date)} to {str(today)}')
    
    dates = {}
    while (date < today):
        logging.debug(f'Querying date: {str(date)}')

        query = select([func.count(tbl_eq_file.c.equipment)]).where(tbl_eq_file.c.pubdate == date)
        count = query.scalar()
        logging.debug(f'Result: {str(count)}')

        if count < TOTAL_EQUIP:
            dates[str(date)] = count

        date = date + datetime.timedelta(1)
    
    return dates

if __name__ == '__main__':
    dates = read_dates()
    if len(dates) == 0:
        logging.info('OK')
    else:
        logging.info(f'Dates with less files:\n{json.dumps(dates, indent=2)}')
