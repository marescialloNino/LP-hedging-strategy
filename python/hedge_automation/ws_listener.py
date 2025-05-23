import asyncio
import json
import logging
import aiohttp
from aiohttp import WSMsgType

logger = logging.getLogger(__name__)

class WebSpreaderListener:
    AMAZON_WS_UPI = "ws://54.249.138.8:8080/wsapi/strat/update"

    def __init__(self):
        self.session = None
        self.ws = None
        self.subscriptions = set()
        self.results = {}
        self._connected = False
        self._task = None
        self._running = False

    async def _initialize(self):
        logger.info(f"Connecting to {self.AMAZON_WS_UPI}")
        if self.session is None:
            self.session = aiohttp.ClientSession()
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.ws = await self.session.ws_connect(self.AMAZON_WS_UPI, heartbeat=30)
                self._connected = True
                logger.info(f"Connected to {self.AMAZON_WS_UPI}")
                for strat_id in self.subscriptions:
                    await self._subscribe_strat(strat_id)
                break
            except Exception as e:
                logger.error(f"WebSocket connection failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    raise Exception("WebSocket listener initialization failed after max retries")

    async def _subscribe_strat(self, strat_id):
        if not self._connected:
            logger.warning(f"Cannot subscribe to {strat_id}, WebSocket not connected")
            return
        try:
            message = {"type": "subscribe", "stratId": strat_id}
            await self.ws.send_json(message)
            logger.info(f"Subscribed to strat_id: {strat_id}")
        except Exception as e:
            logger.error(f"Failed to subscribe to {strat_id}: {e}")

    async def subscribe(self, strat_id):
        self.subscriptions.add(strat_id)
        if self._connected:
            await self._subscribe_strat(strat_id)
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._listen())

    async def _listen(self):
        logger.info("Starting WebSocket listener task")
        while self._running:
            try:
                if not self._connected:
                    await self._initialize()
                async for msg in self.ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            logger.debug(f"Raw WebSocket message: {data}")
                            strat_events = data.get("stratEvents", [])
                            for event in strat_events:
                                strat_id = event.get("stratId")
                                if strat_id not in self.subscriptions:
                                    continue
                                config = event.get("manualOrderConfiguration", {})
                                results = event.get("results", {})
                                self.results[strat_id] = {
                                    "execQty": results.get("execQty", "0"),
                                    "targetSize": config.get("targetSize", 0.0),
                                    "maxOrderSize": config.get("maxOrderSize", 0.0),
                                    "num_child_orders": config.get("numChildOrders", 0),
                                    "state": results.get("state", ""),
                                    "info": results.get("info", "")
                                }
                                logger.info(f"Received update for strat_id {strat_id}: {self.results[strat_id]}")
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse WebSocket message: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing WebSocket message: {e}")
                            continue
                    elif msg.type == WSMsgType.CLOSED:
                        logger.warning("WebSocket connection closed")
                        self._connected = False
                        break
                    elif msg.type == WSMsgType.ERROR:
                        logger.error(f"WebSocket error: {msg}")
                        self._connected = False
                        break
            except Exception as e:
                logger.error(f"WebSocket listener error: {e}", exc_info=True)
                self._connected = False
                if self._running:
                    logger.info("Attempting to reconnect WebSocket")
                    await asyncio.sleep(2)
                    continue

        if self._connected:
            await self.ws.close()
            self._connected = False
        logger.info("WebSocket listener task completed")

    async def get_strat_result(self, strat_id):
        if strat_id not in self.results:
            logger.debug(f"No results yet for strat_id {strat_id}")
            return {}
        return self.results.get(strat_id, {})

    async def stop(self):
        logger.info("Stopping WebSocket listener")
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("Listener task cancelled")
        if self.ws and self._connected:
            await self.ws.close()
        if self.session:
            await self.session.close()
        self._connected = False
        self.subscriptions.clear()
        self.results.clear()
        logger.info("WebSocket listener stopped")