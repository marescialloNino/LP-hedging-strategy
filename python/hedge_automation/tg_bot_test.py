import asyncio
from telegram import Bot
from telegram.error import TelegramError
import os
from hedge_automation.ws_manager import ws_manager
from hedge_automation.bot_reporting import TGMessenger  


async def test_bot():
    TGMessenger.send("Test message from bot", "CM") 

if __name__ == "__main__":
    asyncio.run(test_bot())