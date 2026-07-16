"""ib_market 短路径辅助单测。"""

from ib_market import (
    allocate_proportional,
    extract_realized_pnl,
    streak_flags_from_pnl,
)


def test_extract_realized_pnl_sentinel():
    class R:
        realizedPNL = 1e100
        commission = 1.0

    class F:
        commissionReport = R()

    assert extract_realized_pnl(F()) is None


def test_extract_realized_pnl_value():
    class R:
        realizedPNL = -42.5
        commission = 1.0

    assert extract_realized_pnl(R()) == -42.5


def test_allocate_proportional():
    parts = allocate_proportional(-100.0, [60.0, 40.0])
    assert parts[0] == -60.0
    assert parts[1] == -40.0


def test_streak_flags_scratch():
    # 默认 scratch $10（无 nlv）
    p, l = streak_flags_from_pnl(-5.0, nlv=None)
    assert p is False and l is False
    p, l = streak_flags_from_pnl(-50.0, nlv=None)
    assert p is False and l is True
    p, l = streak_flags_from_pnl(10.0, nlv=None)
    assert p is True and l is False
