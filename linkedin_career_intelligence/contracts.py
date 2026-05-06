from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class TableContract:
    required_columns: tuple[str, ...]
    non_empty_columns: tuple[str, ...] = ()
    unique_columns: tuple[str, ...] = ()
    max_null_rate_by_column: dict[str, float] = field(default_factory=dict)
    sensitive_columns: tuple[str, ...] = ()
    description: str = ""
    owner: str = "LinkedIn member"


@dataclass(frozen=True)
class ContractValidationResult:
    row_count: int
    missing_columns: tuple[str, ...]
    duplicate_rows: int
    null_rates: dict[str, float]
    null_rate_violations: dict[str, float]


def _build_null_mask(series: pd.Series) -> pd.Series:
    if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        normalized = series.astype("string").str.strip()
        return series.isna() | normalized.isna() | normalized.eq("")

    return series.isna()


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

    columns_to_measure = set(contract.non_empty_columns) | set(contract.max_null_rate_by_column)
    null_rates: dict[str, float] = {}
    null_rate_violations: dict[str, float] = {}

    for column in columns_to_measure:
        if column not in df.columns:
            continue

        if df.empty:
            null_rate = 0.0
        else:
            null_mask = _build_null_mask(df[column])
            null_rate = float(null_mask.mean())

        null_rates[column] = null_rate
        threshold = contract.max_null_rate_by_column.get(column, 0.0)
        if null_rate > threshold:
            null_rate_violations[column] = null_rate

    return ContractValidationResult(
        row_count=len(df),
        missing_columns=missing_columns,
        duplicate_rows=duplicate_rows,
        null_rates=dict(sorted(null_rates.items())),
        null_rate_violations=dict(sorted(null_rate_violations.items())),
    )


def assert_contract(
    df: pd.DataFrame,
    contract: TableContract,
) -> ContractValidationResult:
    validation = validate_contract(df, contract)
    if validation.missing_columns:
        missing = ", ".join(validation.missing_columns)
        raise ValueError(f"Missing required columns after transformation: {missing}")
    if validation.duplicate_rows > 0:
        columns = ", ".join(contract.unique_columns)
        raise ValueError(
            f"Duplicate rows found after transformation for unique columns [{columns}]: "
            f"{validation.duplicate_rows}"
        )
    if validation.null_rate_violations:
        violations = ", ".join(
            f"{column}={rate:.2%}"
            for column, rate in validation.null_rate_violations.items()
        )
        raise ValueError(
            "Null/blank rate above allowed threshold after transformation: "
            f"{violations}"
        )
    return validation
