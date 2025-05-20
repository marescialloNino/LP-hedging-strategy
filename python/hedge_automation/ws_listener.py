import pandas as pd
import websockets
import asyncio
import json
import traceback

def today_utc():
    """Return current UTC timestamp as a pandas Timestamp."""
    return pd.Timestamp.now(tz='UTC')

class WebSpreaderListener:
    AMAZON_WS_UPI = 'ws://54.249.138.8:8080/wsapi/strat/update'

    def __init__(self, logger):
        self.listener_task = None
        self.subscriptions = set()
        self.results = {'errors': [], 'last_modified': pd.Timestamp(year=2000,month=1,day=1, tz='UTC')}
        self._logger = logger

    def message_age(self) -> float:
        """
        Age in seconds
        :return: message age
        """
        return (today_utc() - self.results['last_modified']).total_seconds()

    async def _listen(self):
        greeting = 'Allo?'
        self._logger.info('starting listener')
        async with websockets.connect(self.AMAZON_WS_UPI, max_size=4194304) as websocket:
            await websocket.send(greeting)
            while True:
                message = await websocket.recv()
                if message:
                    messages = json.loads(message)
                    events = messages.get('stratEvents', [])
                    self.results['last_modified'] = today_utc()

                    for event in events:
                        strat_id = event.get('stratId', '')
                        if strat_id in self.subscriptions:
                            manual_config = event.get('manualOrderConfiguration', {})
                            results = event.get('results', {})
                            self.results[strat_id] = {
                                'execQty': float(results.get('execQty', 0)),
                                'state': results.get('state', ''),
                                'info': results.get('info', ''),
                                'targetSize': float(manual_config.get('targetSize', 0)),
                                'maxOrderSize': float(manual_config.get('maxOrderSize', 0)),
                                'maxAliveOrderTime': int(manual_config.get('maxAliveOrderTime', 0)),
                                'childOrderDelay': int(manual_config.get('childOrderDelay', 0)),
                                'maxRetryAsLimitOrder': int(manual_config.get('maxRetryAsLimitOrder', 0))
                            }

    def _start_listener(self):
        async def rerun_forever(coro, *args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    msg = traceback.format_exc()
                    self.results['errors'].append(msg)
                    self._logger.warning(f'listener said {msg}')
                    await asyncio.sleep(60)

        def done_listener(task):
            self.listener_task = None

        self.results['errors'].clear()
        self.listener_task = asyncio.create_task(rerun_forever(self._listen))
        self.listener_task.add_done_callback(done_listener)

    def subscribe(self, strat_id):
        self.subscriptions.add(strat_id)
        if self.listener_task is None:
            self._start_listener()

    async def stop_listener(self, strat_id=None):
        if strat_id is not None:
            self.subscriptions.remove(strat_id)
            self.results.pop(strat_id, None)
            if len(self.subscriptions) > 0:
                return
        self.subscriptions.clear()
        if self.listener_task is not None:
            self._logger.info('cancelling listener')
            self.listener_task.cancel()

    def get_strat_result(self, strat_id):
        if strat_id in self.results:
            return self.results.get(strat_id, {})
        else:
            self._logger.warning(f'retrieving non existing strat from listener with state {self.results["errors"]}')
            return {}
        