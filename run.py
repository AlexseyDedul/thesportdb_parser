import asyncio
import logging

from app import create_app
from app.thesportsdb_parser.main_thesporsdb import main


logger = logging.getLogger(__name__)


def run():
    app = create_app()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(app))
        # loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Close application.")


if __name__ == '__main__':
    run()
