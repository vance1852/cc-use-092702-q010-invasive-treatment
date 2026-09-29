"""处置台账服务层错误。"""

from __future__ import annotations


class LedgerError(RuntimeError):
    code = "ledger_error"
    status = 400


class NotFound(LedgerError):
    code = "not_found"
    status = 404


class Conflict(LedgerError):
    code = "conflict"
    status = 409


class InvalidState(LedgerError):
    code = "invalid_state"
    status = 409


class ValidationFailed(LedgerError):
    code = "validation_failed"
    status = 422
