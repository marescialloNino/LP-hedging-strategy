import os
import sys
import aiohttp
import asyncio
import dotenv
from datetime import datetime
import pandas as pd


API_V1 = 'https://api.krystal.app/all/v1/'
EP_POS = 'lp/'

async def position_fetcher(session, addresses, chains):
    """

    :param session: aiohttp.ClientSession
    :param addresses: list of wallet adresses
    :param chains: list of chains
    :return:
    """

    if isinstance(addresses, list):
        add = ','.join(addresses)
    else:
        add = addresses

    url_req = f"{API_V1}{EP_POS}userPositions?addresses={add}&chainIds={chains}"

    try:
        async with session.get(url_req) as resp:
            response = await resp.json()
            return response
    except Exception as err:
        print(f"Error fetching positions: {err}")
        return {}

async def main():
    dotenv.load_dotenv()
    addresses = os.getenv('EVM_WALLET_ADDRESSES')
    chains = os.getenv('KRYSTAL_CHAIN_IDS')
    tz_info = datetime.now().astimezone().tzinfo
    now = pd.Timestamp(datetime.today()).tz_localize(tz_info).tz_convert('UTC')

    async with aiohttp.ClientSession() as session:
        positions = await position_fetcher(session, addresses, chains)

    statsByChain = positions.get('statsByChain', {})
    all_stats = statsByChain.get('all', {})
    headers = ['currentPositionValue', 'totalDepositValue', 'totalWithdrawValue', 'totalFeeEarned', 'feeEarned24h', 'totalGasUsed']
    values = [str(all_stats.get(key, 0)) for key in headers]
    filename = "../lp-data/balance_history.csv"

    if len(values) > 0:
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                f.write(','.join(['date'] + headers) + '\n')
                f.write(f"{now},{','.join(values)}"+ '\n')
        else:
            with open(filename, 'a') as f:
                f.write(f"{now},{','.join(values)}"+ '\n')


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
