import os
import requests
import asyncio
import dotenv


API_V1 = 'https://api.krystal.app/all/v1/'
EP_POS = 'lp/'

async def position_fetcher(session, addresses, chains):
    """

    :param session: aiohttp.ClientSession
    :param addresses: list of wallet adresses
    :param chains: list of chains
    :return:
    """

    url_req = f"{API_V1}{EP_POS}userPositions?addresses={','.join(addresses)}&chainIds={','.join(chains)}"

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

    with requests.Session() as session:
        positions = await position_fetcher(session, addresses, chains)
    statsByChain = positions.get('statsByChain', {})
    all_stats = statsByChain.get('all', {})
    headers = ['currentPositionValue', 'totalDepositValue', 'totalWithdrawValue', 'totalFeeEarned', 'feeEarned24h', 'totalGasUsed']
    values = [all_stats.get(key, 0) for key in headers]
    filename = "balance_tracker.csv"
    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            f.write(','.join(headers) + '\n')
            f.write(','.join(f'{values}:.3f') + '\n')
    else:
        with open(filename, 'a') as f:
            f.write(','.join(f'{values}:.3f') + '\n')





if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())