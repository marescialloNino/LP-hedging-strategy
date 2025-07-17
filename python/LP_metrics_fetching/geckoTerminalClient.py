import requests
import time
from typing import Dict, List, Optional

class GeckoTerminalClient:
    def __init__(self):
        self.base_url = "https://api.geckoterminal.com/api/v2"
        self.session = requests.Session()
        self.rate_limit = 30  # calls per minute
        self.calls = []

    def _rate_limit_check(self):
        """Enforce rate limiting: 30 calls per minute."""
        current_time = time.time()
        self.calls = [call for call in self.calls if current_time - call < 60]
        if len(self.calls) >= self.rate_limit:
            sleep_time = 60 - (current_time - self.calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.calls.append(current_time)

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make an API request with rate limiting."""
        self._rate_limit_check()
        try:
            response = self.session.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {endpoint}: {e}")
            return {}

    def fetch_pool_metrics(self, network: str, pool_address: str) -> Optional[Dict]:
        """Fetch TVL and 24h volume for a specific pool."""
        endpoint = f"/networks/{network}/pools/{pool_address}"
        response = self._make_request(endpoint)
        if not response or 'data' not in response or 'attributes' not in response['data']:
            print(f"No data found for pool {pool_address} on {network}")
            return None

        attributes = response['data']['attributes']
        try:
            tvl = float(attributes.get('reserve_in_usd', 0))
            volume = float(attributes.get('volume_usd', {}).get('h24', 0))
            return {
                'network': network,
                'pool_address': pool_address,
                'tvl_usd': tvl,
                'volume_24h_usd': volume
            }
        except (ValueError, TypeError) as e:
            print(f"Error processing metrics for pool {pool_address} on {network}: {e}")
            return None

    def fetch_multi_pool_metrics(self, network: str, pool_addresses: List[str]) -> List[Dict]:
        """Fetch TVL and 24h volume for multiple pools in a single request."""
        if not pool_addresses:
            return []

        # Join pool addresses into a comma-separated string
        pool_addresses_str = ",".join(pool_addresses)
        endpoint = f"/networks/{network}/pools/multi/{pool_addresses_str}"
        response = self._make_request(endpoint)

        results = []
        if not response or 'data' not in response:
            print(f"No data found for pools on {network}")
            return results

        for pool_data in response['data']:
            try:
                attributes = pool_data.get('attributes', {})
                pool_address = attributes.get('address', '')
                tvl = float(attributes.get('reserve_in_usd', 0))
                volume = float(attributes.get('volume_usd', {}).get('h24', 0))
                results.append({
                    'network': network,
                    'pool_address': pool_address,
                    'tvl_usd': tvl,
                    'volume_24h_usd': volume
                })
            except (ValueError, TypeError) as e:
                print(f"Error processing metrics for pool {pool_address} on {network}: {e}")
                continue

        return results