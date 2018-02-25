import asyncio
from time import time

class Throttler:
    """Generic asynchronous function object, useful for any kind of
    throttling.

    Run is registered (and throttled if necessary) upon call of the created
    function object.

    Parameters
    ----------
    ts_count : int
        Maximum number of times to run in the `tlim_s` time limit.
    tlim_s : float
        Time limit in seconds in which `ts_count` amount of runs are permitted.
    """

    def __init__(self, ts_count, tlim_s):
        self._timestamps = []
        self._lock = asyncio.Lock()
        self._ts_count = ts_count
        self._tlim_s = tlim_s

    async def _do_throttle(self):
        now = time()
        if len(self._timestamps) >= self._ts_count:
            self._timestamps = [x for x in self._timestamps
                                if now - x < self._tlim_s]
            if len(self._timestamps) >= self._ts_count:
                sleep_s = self._tlim_s - (now - self._timestamps[0])
                # L.debug("throttler going to sleep ...")
                await asyncio.sleep(sleep_s)
                # L.debug("throttler woke up")
                self._timestamps = self._timestamps[1:] + [time()]
            else:
                self._timestamps.append(now)
        else:
            self._timestamps.append(now)

    async def __call__(self):
        async with self._lock:
            await self._do_throttle()
