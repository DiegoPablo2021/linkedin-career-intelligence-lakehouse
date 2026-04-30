from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class TableContract:
    required_columns: tuple[str, ...]
    non_empty_columns: tuple[str, ...] = ()
    unique_columns: tuple[str, ...] = ()
    sensitive_columns: tuple[str, ...] = ()
    description: str = ""
    owner: str = "LinkedIn member"


@dataclass(frozen=True)
class ContractValidationResult:
    row_count: int
    missing_columns: tuple[str, ...]
    duplicate_rows: int


def validate_contract(
    df: pd.DataFrame,
    contract: TableContract,
) -> ContractValidationResult:
    missing_columns = tuple(
        column for column in contract.required_columns if column not in df.columns
    )
    duplicate_rows = 0
    if contract.unique_columns:
        duplicate_rows = int(df.duplicated(subset=list(contract.unique_columns)).sum())

    return ContractValidationResult(
        row_count=len(df),
        missing_columns=missing_columns,
        duplicate_rows=duplicate_rows,
    )


def assert_contract(
    df: pd.DataFrame,
    contract: TableContract,
) -> ContractValidationResult:
    validation = validate_contract(df, contract)
    if validation.missing_columns:
        missing = ", ".join(validation.missing_columns)
        raise ValueError(f"Missing required columns after transformation: {missing}")
    return validation
