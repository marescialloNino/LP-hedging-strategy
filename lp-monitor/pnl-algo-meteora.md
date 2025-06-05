Meteora Position PnL Calculation Steps

This document outlines the steps to calculate the Profit and Loss (PnL) for a liquidity position on the Meteora platform in USD, as implemented in the calculateMeteoraPositionPNLUsd function. The process accounts for deposits, withdrawals, and fee claims, including reinvested fees.
Step 1: Fetch Token Metadata

Retrieve metadata for the two tokens in the position (Token X and Token Y) to get their decimal places.
Purpose: Adjust raw token amounts to actual quantities (e.g., divide by 10^decimals).

Step 2: Fetch and Sort Events

Fetch three types of events for the position:
Deposits: Funds added to the position.
Withdrawals: Funds removed from the position.
Fee Claims: Fees earned from the position.


Combine all events into a single list and sort them by timestamp (earliest to latest).
Purpose: Process events in chronological order to track position changes accurately.

Step 3: Initialize Tracking Variables

Set up variables to track:
Cumulative Claimed Tokens: Total raw amounts of Token X and Token Y from fee claims.
Available Claimed Tokens: Claimed tokens available for reinvestment.
USD Values:
capitalDepositsUsd: Total USD value of new capital deposited.
totalDepositUsd: Total USD value of all deposits (including reinvested fees).
totalFeeRewardUsd: Total USD value of all fee claims.
totalWithdrawalUsd: Total USD value of all withdrawals.

Purpose: Track token and USD flows to differentiate new capital from reinvested fees.

Step 4: Process Each Event
Loop through events and update tracking variables based on event type:

4.1 Claim Fees Events

Add claimed token amounts to cumulative and available token totals.
Add the USD value of fees (token_x_usd_amount + token_y_usd_amount) to totalFeeRewardUsd.
Purpose: Track earned fees and make them available for reinvestment.

4.2 Deposit Events

Adjust token amounts for decimals to get actual quantities (e.g., token_x_amount / 10^tokenXDecimals).
Calculate reinvestment ratios:
Ratio = min(available claimed tokens / deposit tokens, 1) for each token.


Split deposit USD value into:
Reinvested fees: token_x_usd_amount * reinvestedRatioX (and similarly for Token Y).
New capital: token_x_usd_amount * (1 - reinvestedRatioX).


Update totals:
Add new capital to capitalDepositsUsd.
Add total deposit value to totalDepositUsd.


Reduce available claimed tokens by reinvested amounts.
Purpose: Distinguish between new capital and reinvested fees in deposits.

4.3 Withdrawal Events

Add the USD value of the withdrawal (token_x_usd_amount + token_y_usd_amount) to totalWithdrawalUsd.
Purpose: Track funds removed from the position.

Step 5: Calculate Reinvested Fees

Compute reinvested fees as the difference between total deposits and new capital:
reinvestedFeesUsd = totalDepositUsd - capitalDepositsUsd


Purpose: Isolate the USD value of fees reinvested into the position.

Step 6: Calculate Total Inflows

Sum the new capital, total fees, and reinvested fees:
totalInflowsUsd = capitalDepositsUsd + totalFeeRewardUsd + reinvestedFeesUsd


Purpose: Determine the total money invested in the position.

Step 7: Compute PnL Components

Calculate key PnL metrics:
Withdrawal Ratio: min(totalWithdrawalUsd / totalInflowsUsd, 1)
Caps at 1 to ensure withdrawals don’t exceed inflows.


Withdrawn Capital: capitalDepositsUsd * withdrawalRatio
Portion of original capital withdrawn.


Realized PnL: totalWithdrawalUsd - withdrawnCapitalUsd
Profit or loss from withdrawals.


Remaining Capital: capitalDepositsUsd - withdrawnCapitalUsd
Capital still in the position.


Unrealized PnL: currentPositionValueUsd - remainingCapitalUsd
Profit or loss on the current position value.


Net PnL: (currentPositionValueUsd + totalWithdrawalUsd) - capitalDepositsUsd
Total profit or loss (realized + unrealized).



Step 8: Return Results

Return an object with:
realizedPNLUsd: Profit/loss from withdrawals.
unrealizedPNLUsd: Profit/loss from current position value.
netPNLUsd: Total profit/loss.
capitalDepositsUsd: New capital invested.
reinvestedFeesUsd: Fees reinvested.
totalFeeRewardUsd: Total fees earned.


Purpose: Provide a comprehensive breakdown of the position’s performance.

Notes

The function assumes currentPositionValueUsd is provided as input, reflecting the position’s current value in USD.
The process ensures accurate PnL by separating new capital from reinvested fees, avoiding double-counting.
The same logic applies to calculateMeteoraPositionPNLTokenB, but converts values to Token Y units using historical or current token prices.

