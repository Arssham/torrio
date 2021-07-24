import asyncio

import uvloop

from . import main

uvloop.install()

loop = asyncio.get_event_loop()

loop.run_until_complete(main())
