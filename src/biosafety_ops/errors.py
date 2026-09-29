"""处置台账的领域错误。

均继承 ValueError，兼容旧调用方对 ValueError 的宽泛捕获。
"""
from __future__ import annotations


class LedgerError(ValueError):
    """台账规则错误基类。"""


class ValidationFailed(LedgerError):
    """输入不满足领域校验。"""


class Conflict(LedgerError):
    """幂等键对应不同内容，或无变化的重复修订。"""


class InvalidState(LedgerError):
    """台账当前状态不允许该操作。"""
