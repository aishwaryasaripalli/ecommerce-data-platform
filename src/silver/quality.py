"""
Data quality checks: small, readable assertions about a table, run before it's trusted.

Each check counts the rows that break a rule. Severity decides what happens next:
    error : the data is wrong. The Silver build fails, so nobody downstream reads it.
    warn  : the data is odd but real (Olist really has 8 "delivered" orders with no
            delivery date). Recorded and shown, but the build continues.

Every result is also saved to the Delta table s3a://lake/_quality/silver_checks, so
you can chart data quality over time. These are the same ideas dbt tests use in
Phase 6 (not_null, unique, accepted_values, relationships), written in PySpark.

USAGE
    dq = QualitySuite()
    dq.unique("orders", df, ["order_id"])
    dq.expect("orders", df, "delivery after purchase",
              "order_delivered_customer_date >= order_purchase_timestamp", severity="warn")
    dq.print_report(); dq.raise_on_errors()
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    failing_rows: int

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


class DataQualityError(Exception):
    pass


@dataclass
class QualitySuite:
    results: list[CheckResult] = field(default_factory=list)

    def _record(self, table: str, check: str, severity: str, failing_rows: int) -> None:
        assert severity in ("error", "warn")
        self.results.append(CheckResult(table, check, severity, int(failing_rows)))

    # --- the checks -------------------------------------------------------

    def not_null(self, table: str, df: DataFrame, cols: list[str], severity: str = "error") -> None:
        for c in cols:
            self._record(table, f"not_null({c})", severity, df.filter(F.col(c).isNull()).count())

    def unique(self, table: str, df: DataFrame, cols: list[str], severity: str = "error") -> None:
        """Number of rows beyond the first for any repeated key."""
        dupes = df.groupBy(*cols).count().filter("count > 1").agg(F.sum(F.col("count") - 1)).first()[0]
        self._record(table, f"unique({', '.join(cols)})", severity, dupes or 0)

    def accepted_values(self, table: str, df: DataFrame, col: str, values: list[str],
                        severity: str = "error") -> None:
        bad = df.filter(F.col(col).isNull() | ~F.col(col).isin(values)).count()
        self._record(table, f"accepted_values({col})", severity, bad)

    def expect(self, table: str, df: DataFrame, name: str, condition_sql: str,
               severity: str = "error") -> None:
        """Rows where a SQL condition is false. Rows where it's NULL (missing data) don't count."""
        self._record(table, name, severity, df.filter(f"NOT ({condition_sql})").count())

    def relationship(self, table: str, df: DataFrame, col: str, parent_name: str,
                     parent: DataFrame, parent_col: str, severity: str = "error") -> None:
        """Every non-null df.col exists in parent.parent_col (a foreign key)."""
        orphans = (df.select(col).filter(F.col(col).isNotNull())
                   .join(parent.select(F.col(parent_col).alias(col)), col, "left_anti").count())
        self._record(table, f"relationship({col} -> {parent_name}.{parent_col})", severity, orphans)

    def row_count_equals(self, table: str, df: DataFrame, expected: int, why: str,
                         severity: str = "error") -> None:
        self._record(table, f"row_count = {why}", severity, abs(df.count() - expected))

    # --- reporting --------------------------------------------------------

    def print_report(self) -> None:
        print(f"\n{'table':<22} {'check':<58} {'severity':<8} result")
        print("-" * 100)
        for r in self.results:
            status = "PASS" if r.passed else f"FAIL ({r.failing_rows:,} rows)"
            print(f"{r.table:<22} {r.check:<58} {r.severity:<8} {status}")
        errors = [r for r in self.results if r.severity == "error" and not r.passed]
        warns = [r for r in self.results if r.severity == "warn" and not r.passed]
        print(f"\n{len(self.results)} checks: {len(errors)} errors, {len(warns)} warnings")

    def to_dataframe(self, spark, run_id: str) -> DataFrame:
        rows = [(run_id, r.table, r.check, r.severity, r.failing_rows, r.passed) for r in self.results]
        return (spark.createDataFrame(rows, "run_id string, table string, check string, severity string, "
                                            "failing_rows long, passed boolean")
                .withColumn("checked_at", F.current_timestamp()))

    def raise_on_errors(self) -> None:
        errors = [r for r in self.results if r.severity == "error" and not r.passed]
        if errors:
            raise DataQualityError(
                "Silver failed data quality: " + "; ".join(f"{r.table}.{r.check}" for r in errors))
