"""risk_metrics 纯函数与快照口径单测。"""

import pytest

from risk_metrics import (
    _position_r,
    _position_risk_usd,
    risk_light_and_budget,
)


def test_position_risk_long():
    assert _position_risk_usd(100, 97, 10) == pytest.approx(30.0)


def test_position_r_long_and_3r():
    r = _position_r("LONG", 100.0, 97.0, 109.0)
    assert r == pytest.approx(3.0)


def test_risk_light_yellow_halves_budget():
    light, budget = risk_light_and_budget(0.06, 0, 100_000)
    assert light == "🟡"
    assert budget == pytest.approx(150.0)  # 0.3%/2


def test_risk_light_red_blocks():
    light, budget = risk_light_and_budget(0.0, 5, 100_000)
    assert light == "🔴"
    assert budget == 0.0


def test_risk_light_green():
    light, budget = risk_light_and_budget(0.01, 0, 100_000)
    assert light == "🟢"
    assert budget == pytest.approx(300.0)
