from decimal import Decimal
from types import SimpleNamespace

import pytest

from bot.arbitrum_pipeline import ArbitrumPipeline, ArbitrumRoute, RouteEvaluation
from bot.quotes import QuoteError


def _pipeline():
    pipeline = ArbitrumPipeline.__new__(ArbitrumPipeline)
    pipeline.w3 = object()
    pipeline.base_symbol = "USDC"
    pipeline._tokens = {"USDC": SimpleNamespace(decimals=6)}
    pipeline._addresses = SimpleNamespace(arb_gas_info="0x000000000000000000000000000000000000006C")
    pipeline.settings = SimpleNamespace(gas_safety_margin_bps=1000, max_gas_cost_usd=Decimal("10"))
    return pipeline


def _evaluation():
    return RouteEvaluation(ArbitrumRoute(tuple()), tuple(), 1_000_000, 1_020_000, 500, 1_000, 5_000, 18_500)


def test_post_simulation_gas_recheck_rejects_route(monkeypatch):
    pipeline = _pipeline()
    monkeypatch.setattr("bot.arbitrum_pipeline.estimate_arbitrum_gas_cost", lambda *args, **kwargs: SimpleNamespace(gas_cost_usd=Decimal("0.020")))

    final = pipeline.reprice_after_simulation(_evaluation(), "0x1234", 700_000, Decimal("3000"))

    assert final.gas_cost_asset == 20_000
    assert final.expected_net_profit == -500
    assert not final.is_profitable


def test_post_simulation_gas_recheck_rejects_excessive_gas(monkeypatch):
    pipeline = _pipeline()
    monkeypatch.setattr("bot.arbitrum_pipeline.estimate_arbitrum_gas_cost", lambda *args, **kwargs: SimpleNamespace(gas_cost_usd=Decimal("11")))

    with pytest.raises(QuoteError, match="post-simulation gas"):
        pipeline.reprice_after_simulation(_evaluation(), "0x1234", 700_000, Decimal("3000"))
