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
        self.results = {'errors': [], 'last_modified': pd.Timestamp(year=2000, month=1, day=1, tz='UTC')}
        self._logger = logger
        self._connected = False

    def message_age(self) -> float:
        """Return message age in seconds."""
        return (today_utc() - self.results['last_modified']).total_seconds()

    async def _listen(self):
        greeting = 'Allo?'
        self._logger.info('Starting WebSocket listener')
        retry_delay = 5  # Initial retry delay in seconds
        max_retry_delay = 60  # Maximum retry delay

        while True:
            try:
                async with websockets.connect(
                    self.AMAZON_WS_UPI,
                    ping_interval=30,
                    ping_timeout=10
                ) as websocket:
                    self._connected = True
                    self._logger.info(f"Connected to {self.AMAZON_WS_UPI}")
                    await websocket.send(greeting)
                    retry_delay = 5  # Reset delay on successful connection

                    while True:
                        try:
                            message = await websocket.recv()
                            if message:
                                self._logger.debug(f"Received message: {message}")
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
                                        self._logger.debug(f"Processed event for strat_id {strat_id}: {self.results[strat_id]}")
                        except websockets.exceptions.ConnectionClosed:
                            self._connected = False
                            self._logger.warning("WebSocket connection closed, attempting to reconnect")
                            break
                        except json.JSONDecodeError as e:
                            self._logger.error(f"Failed to parse message: {e}")
                            self.results['errors'].append(str(e))
                        except Exception as e:
                            self._logger.error(f"Error processing message: {e}")
                            self.results['errors'].append(str(e))

            except Exception as e:
                self._connected = False
                self._logger.error(f"WebSocket connection failed: {e}")
                self.results['errors'].append(str(e))
                self._logger.info(f"Retrying connection in {retry_delay} seconds")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff

    def _start_listener(self):
        async def rerun_forever(coro, *args, **kwargs):
            try:
                await coro(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception:
                msg = traceback.format_exc()
                self.results['errors'].append(msg)
                self._logger.error(f"Listener crashed: {msg}")

        def done_listener(task):
            self.listener_task = None
            self._connected = False
            self._logger.info("Listener task completed")

        self.results['errors'].clear()
        if self.listener_task is None:
            self.listener_task = asyncio.create_task(rerun_forever(self._listen))
            self.listener_task.add_done_callback(done_listener)

    def subscribe(self, strat_id):
        if strat_id not in self.subscriptions:
            self._logger.info(f"Subscribing to strat_id: {strat_id}")
            self.subscriptions.add(strat_id)
            if self.listener_task is None or not self._connected:
                self._start_listener()
        else:
            self._logger.debug(f"Already subscribed to strat_id: {strat_id}")

    async def stop_listener(self, strat_id=None):
        if strat_id is not None:
            if strat_id in self.subscriptions:
                self.subscriptions.remove(strat_id)
                self.results.pop(strat_id, None)
                self._logger.info(f"Unsubscribed from strat_id: {strat_id}")
            if len(self.subscriptions) > 0:
                return
        self.subscriptions.clear()
        if self.listener_task is not None:
            self._logger.info('Cancelling WebSocket listener')
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass
            self.listener_task = None
            self._connected = False
        self.results = {'errors': [], 'last_modified': pd.Timestamp(year=2000, month=1, day=1, tz='UTC')}

    def get_strat_result(self, strat_id):
        if strat_id in self.results:
            return self.results.get(strat_id, {})
        else:
            self._logger.warning(f"Retrieving non-existing strat_id {strat_id} from listener")
            return {}