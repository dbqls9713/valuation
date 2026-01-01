"""
Validation runner with reporting.

Orchestrates multiple validation checks and provides
consistent output formatting.

Supports two types of validations:
- checks: Must pass for validation to succeed (code correctness)
- warnings: Informational, failures don't fail validation (data quality)
"""
from dataclasses import dataclass
import logging
from typing import Any, Callable, Union

from data.shared.validation.base import CheckResult

logger = logging.getLogger(__name__)

ValidatorFn = Callable[..., Union[CheckResult, list[CheckResult]]]


@dataclass
class _RegisteredCheck:
  """Internal representation of a registered validation."""

  check_id: str
  validator_fn: ValidatorFn
  args: tuple[Any, ...]
  kwargs: dict[str, Any]
  is_warning: bool


@dataclass
class ValidationResult:
  """Result of a validation with its type."""

  result: CheckResult
  is_warning: bool


class ValidationRunner:
  """
  Run multiple validation checks and report results.

  Supports two types:
  - add_check(): Must pass (code correctness validation)
  - add_warning(): Informational (data quality monitoring)

  Usage:
    runner = ValidationRunner('silver_layer')

    # Must pass - code correctness
    runner.add_check('schema', SchemaValidator(schema).validate, df)

    # Informational - data quality
    runner.add_warning('ytd_identity', YTDValidator().validate, facts, metrics)

    ok = runner.run()  # Only checks affect this
    runner.print_summary()
  """

  def __init__(self, layer_name: str):
    self.layer_name = layer_name
    self._registered: list[_RegisteredCheck] = []
    self.results: list[ValidationResult] = []

  def add_check(
      self,
      check_id: str,
      validator_fn: ValidatorFn,
      *args: Any,
      **kwargs: Any,
  ) -> None:
    """
    Register a validation check (must pass).

    Args:
      check_id: Identifier for this check
      validator_fn: Function that returns CheckResult or list[CheckResult]
      *args: Positional arguments for validator_fn
      **kwargs: Keyword arguments for validator_fn
    """
    reg = _RegisteredCheck(check_id, validator_fn, args, kwargs, False)
    self._registered.append(reg)

  def add_warning(
      self,
      check_id: str,
      validator_fn: ValidatorFn,
      *args: Any,
      **kwargs: Any,
  ) -> None:
    """
    Register a warning check (informational, doesn't fail validation).

    Args:
      check_id: Identifier for this check
      validator_fn: Function that returns CheckResult or list[CheckResult]
      *args: Positional arguments for validator_fn
      **kwargs: Keyword arguments for validator_fn
    """
    self._registered.append(
        _RegisteredCheck(check_id, validator_fn, args, kwargs, is_warning=True))

  def run(self) -> bool:
    """
    Execute all registered validations.

    Returns:
      True if all checks (not warnings) pass, False otherwise
    """
    self.results = []

    for reg in self._registered:
      try:
        result = reg.validator_fn(*reg.args, **reg.kwargs)
        if isinstance(result, list):
          for r in result:
            self.results.append(ValidationResult(r, reg.is_warning))
        else:
          self.results.append(ValidationResult(result, reg.is_warning))
      except Exception as e:  # pylint: disable=broad-except
        self.results.append(
            ValidationResult(
                CheckResult(
                    name=reg.check_id,
                    ok=False,
                    details=f'Exception: {type(e).__name__}: {e}',
                ), reg.is_warning))

    # Only non-warning checks affect pass/fail
    checks_only = [r for r in self.results if not r.is_warning]
    return all(r.result.ok for r in checks_only)

  def print_summary(self, verbose: bool = False) -> None:
    """
    Print validation summary to stdout.

    Args:
      verbose: If True, print details for passing checks too
    """
    checks = [r for r in self.results if not r.is_warning]
    warnings = [r for r in self.results if r.is_warning]

    checks_passed = sum(1 for r in checks if r.result.ok)
    checks_failed = sum(1 for r in checks if not r.result.ok)
    warnings_ok = sum(1 for r in warnings if r.result.ok)
    warnings_issue = sum(1 for r in warnings if not r.result.ok)

    print()
    print('=' * 70)
    print(f'=== {self.layer_name} Validation Summary ===')
    print('=' * 70)

    # Print checks first
    if checks:
      print('--- Checks (must pass) ---')
      for vr in checks:
        status = '✓ OK  ' if vr.result.ok else '✗ FAIL'
        print(f'{status} {vr.result.name}')
        show_details = (not vr.result.ok or verbose or
                        'pit' in vr.result.name.lower())
        if show_details:
          print(f'       {vr.result.details}')

    # Print warnings
    if warnings:
      print('--- Warnings (data quality) ---')
      for vr in warnings:
        status = '✓ OK  ' if vr.result.ok else '⚠ WARN'
        print(f'{status} {vr.result.name}')
        # Always show warning details
        print(f'       {vr.result.details}')

    print('=' * 70)
    print(f'Checks: {checks_passed}/{len(checks)} passed', end='')
    if checks_failed > 0:
      print(f' ({checks_failed} FAILED)')
    else:
      print()

    if warnings:
      print(f'Warnings: {warnings_ok}/{len(warnings)} OK', end='')
      if warnings_issue > 0:
        print(f' ({warnings_issue} issues)')
      else:
        print()
    print('=' * 70)

  def log_summary(self) -> None:
    """Log validation summary using logger."""
    checks = [r for r in self.results if not r.is_warning]
    checks_passed = sum(1 for r in checks if r.result.ok)
    checks_failed = sum(1 for r in checks if not r.result.ok)

    logger.info('')
    logger.info('=' * 70)
    logger.info('%s Validation: %d/%d checks passed', self.layer_name,
                checks_passed, len(checks))
    logger.info('=' * 70)

    for vr in self.results:
      if vr.is_warning:
        status = '✓' if vr.result.ok else '⚠'
        level = logging.INFO if vr.result.ok else logging.WARNING
      else:
        status = '✓' if vr.result.ok else '✗'
        level = logging.INFO if vr.result.ok else logging.ERROR
      logger.log(level, '%s %s: %s', status, vr.result.name, vr.result.details)

    if checks_failed > 0:
      logger.error('%d checks FAILED', checks_failed)

  @property
  def all_passed(self) -> bool:
    """Return True if all checks (not warnings) passed."""
    checks = [r for r in self.results if not r.is_warning]
    return all(r.result.ok for r in checks)

  @property
  def failed_checks(self) -> list[CheckResult]:
    """Return list of failed checks (not warnings)."""
    return [
        r.result for r in self.results if not r.is_warning and not r.result.ok
    ]

  @property
  def warning_issues(self) -> list[CheckResult]:
    """Return list of warning issues."""
    return [r.result for r in self.results if r.is_warning and not r.result.ok]
