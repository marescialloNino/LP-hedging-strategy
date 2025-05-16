import asyncio
import pandas as pd
import logging
import json
from pywebio.output import put_markdown, put_code, toast
from common.utils import execute_hedge_trade
from common.path_config import HEDGING_LATEST_CSV

logger = logging.getLogger('hedge_execution')

class HedgeActions:
    def __init__(self, order_sender):
        self.order_sender = order_sender
        self.hedge_processing = {}

    async def handle_hedge_click(self, token, rebalance_value, action):
        if self.hedge_processing.get(token, False):
            toast(f"Hedge already in progress for {token}", duration=5, color="warning")
            return

        self.hedge_processing[token] = True
        try:
            signed_rebalance_value = rebalance_value if action == "buy" else -rebalance_value if action == "sell" else 0.0
            result = await execute_hedge_trade(token, signed_rebalance_value, self.order_sender)
            
            if result['success']:
                put_markdown(f"### Hedge Order Request for {result['token']}")
                put_code(json.dumps(result['request'], indent=2), language='json')
                toast(f"Hedge trade triggered for {result['token']}", duration=5, color="success")
            else:
                toast(f"Failed to generate hedge order for {result['token']}", duration=5, color="error")
        except Exception as e:
            logger.error(f"Exception in handle_hedge_click for {token}: {str(e)}")
            toast(f"Error processing hedge for {token}", duration=5, color="error")
        finally:
            self.hedge_processing[token] = False

    async def handle_close_hedge(self, token, hedged_qty, hedging_df=None):
        if self.hedge_processing.get(token, False):
            toast(f"Close hedge already in progress for {token}", duration=5, color="warning")
            return

        self.hedge_processing[token] = True
        try:
            close_qty = -hedged_qty
            result = await execute_hedge_trade(token, close_qty, self.order_sender)
            
            if result['success']:
                put_markdown(f"### Close Hedge Order Request for {result['token']}")
                put_code(json.dumps(result['request'], indent=2), language='json')
                toast(f"Close hedge triggered for {result['token']}", duration=5, color="success")
                if hedging_df is not None:
                    ticker = f"{token}USDT"
                    hedging_df.loc[hedging_df["symbol"] == ticker, "quantity"] = 0
                    hedging_df.loc[hedging_df["symbol"] == ticker, "amount"] = 0
                    hedging_df.loc[hedging_df["symbol"] == ticker, "funding_rate"] = 0
                    hedging_df.to_csv(HEDGING_LATEST_CSV, index=False)
                    logger.info(f"Updated {HEDGING_LATEST_CSV} for {ticker}")
            else:
                toast(f"Failed to close hedge for {result['token']}", duration=5, color="error")
        except Exception as e:
            logger.error(f"Exception in handle_close_hedge for {token}: {str(e)}")
            toast(f"Error processing close hedge for {token}", duration=5, color="error")
        finally:
            self.hedge_processing[token] = False

    async def handle_close_all_hedges(self, token_summary, hedging_df, hedging_error):
        if any(self.hedge_processing.values()):
            toast("Hedge or close operation in progress, please wait", duration=5, color="warning")
            return

        if hedging_error:
            toast("Cannot close hedges due to hedging data fetch error", duration=5, color="error")
            logger.warning("Skipped close all hedges due to hedging_error")
            return

        if token_summary.empty or token_summary["quantity"].eq(0).all():
            toast("No hedge positions to close", duration=5, color="info")
            return

        results = []
        for _, row in token_summary.iterrows():
            token = row["Token"].replace("USDT", "").strip()
            hedged_qty = row["quantity"]
            if hedged_qty != 0:
                self.hedge_processing[token] = True
                try:
                    close_qty = -hedged_qty
                    result = await execute_hedge_trade(token, close_qty, self.order_sender)
                    if result['success'] and hedging_df is not None:
                        ticker = f"{token}USDT"
                        hedging_df.loc[hedging_df["symbol"] == ticker, "quantity"] = 0
                        hedging_df.loc[hedging_df["symbol"] == ticker, "amount"] = 0
                        hedging_df.loc[hedging_df["symbol"] == ticker, "funding_rate"] = 0
                        hedging_df.to_csv(HEDGING_LATEST_CSV, index=False)
                        logger.info(f"Updated {HEDGING_LATEST_CSV} for {ticker}")
                    results.append(result)
                except Exception as e:
                    logger.error(f"Exception closing hedge for {token}: {str(e)}")
                    results.append({'success': False, 'token': token})
                finally:
                    self.hedge_processing[token] = False

        success_count = sum(1 for r in results if r['success'])
        if success_count == len(results) and results:
            toast("All hedge positions closed successfully", duration=5, color="success")
        elif results:
            toast(f"Closed {success_count}/{len(results)} hedge positions", duration=5, color="error")
        else:
            toast("No hedge positions were processed", duration=5, color="info")
        
        for result in results:
            if result['success']:
                put_markdown(f"### Close Hedge Order Request for {result['token']}")
                put_code(json.dumps(result['request'], indent=2), language='json')