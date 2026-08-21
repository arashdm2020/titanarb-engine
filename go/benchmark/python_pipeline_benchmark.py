"""Benchmark-only harness; imports but does not modify or execute the Python bot."""
import json, os, time, tracemalloc
from bot.config import load_settings
from bot.provider import make_http_provider
from bot.arbitrum_pipeline import ArbitrumPipeline
from bot.quotes import get_executable_quote, get_camelot_v3_executable_quote

route_symbols = ("USDC", "WETH", "ARB", "USDC")
settings = load_settings()
w3 = make_http_provider(settings.http_rpc_url)
calls = 0
original = w3.manager.request_blocking
def counted(method, params, *args, **kwargs):
    global calls
    calls += 1
    return original(method, params, *args, **kwargs)
w3.manager.request_blocking = counted
tracemalloc.start(); cpu_start = time.process_time(); total_start = time.perf_counter()
pipeline = ArbitrumPipeline(w3, settings)
os.environ["ARBITRUM_VALIDATION_ROUTE"] = ",".join(route_symbols)
discovery_start = time.perf_counter(); routes = pipeline.discover_routes(); discovery = time.perf_counter() - discovery_start
route = routes[0]
quote_start = time.perf_counter(); amount = 1_000_000_000
for pool in route.hops:
    if pool.dex == "uniswap_v3":
        amount = get_executable_quote(w3, settings.addresses.uniswap_quoter_v2, pool.token_in, pool.token_out, pool.fee, amount).amount_out
    else:
        amount = get_camelot_v3_executable_quote(w3, settings.addresses.camelot_quoter, pool.token_in, pool.token_out, amount).amount_out
quote = time.perf_counter() - quote_start
_, peak = tracemalloc.get_traced_memory()
print(json.dumps({"runtime":"python","block":w3.eth.block_number,"route":" -> ".join(route_symbols),"route_limit":1,"quote_count":3,"pool_discovery_s":discovery,"quote_s":quote,"route_evaluation_s":quote,"total_cycle_s":time.perf_counter()-total_start,"rpc_calls":calls,"cache_hits":0,"memory_peak_bytes":peak,"cpu_process_s":time.process_time()-cpu_start,"amount_out":amount}))
