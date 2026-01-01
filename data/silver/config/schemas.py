"""
Schema definitions for Silver datasets.

Silver layer contains normalized data only:
- facts_long: YTD values as-is from SEC filings
- prices_daily: Daily prices from Stooq
- companies: Company metadata

Note: YTD->quarterly and TTM calculations are done in Gold layer.
"""
from data.silver.core.dataset import ColumnSpec
from data.silver.core.dataset import DatasetSchema

FACTS_LONG_SCHEMA = DatasetSchema(
    name='facts_long',
    description=
    'All filed versions of SEC XBRL facts, filtered by metric_specs (for PIT)',
    columns=[
        ColumnSpec('cik10',
                   'str',
                   nullable=False,
                   description='CIK padded to 10 digits'),
        ColumnSpec('metric',
                   'str',
                   nullable=False,
                   description='Metric name (CFO, CAPEX, SHARES)'),
        ColumnSpec('fiscal_year',
                   'int64',
                   nullable=False,
                   description='Calculated fiscal year from company FYE'),
        ColumnSpec('fiscal_quarter',
                   'str',
                   nullable=False,
                   description='Fiscal quarter (Q1, Q2, Q3, Q4) based on FYE'),
        ColumnSpec('filed',
                   'datetime64[ns]',
                   nullable=False,
                   description='Filing date'),
        ColumnSpec('end',
                   'datetime64[ns]',
                   nullable=False,
                   description='Period end date'),
        ColumnSpec('fy',
                   'int64',
                   nullable=False,
                   description='Fiscal year from SEC filing'),
        ColumnSpec('fp',
                   'str',
                   nullable=False,
                   description='Original fiscal period (Q1, Q2, Q3, FY)'),
        ColumnSpec('namespace',
                   'str',
                   nullable=False,
                   description='XBRL namespace (us-gaap)'),
        ColumnSpec('tag', 'str', nullable=False, description='XBRL tag name'),
        ColumnSpec('unit',
                   'str',
                   nullable=False,
                   description='Unit (USD, shares)'),
        ColumnSpec('form',
                   'str',
                   nullable=False,
                   description='Form type (10-K, 10-Q)'),
        ColumnSpec('val',
                   'float64',
                   nullable=False,
                   description='Value (YTD for cash flow items)'),
    ],
    primary_key=['cik10', 'metric', 'fiscal_year', 'fiscal_quarter', 'filed'])

PRICES_DAILY_SCHEMA = DatasetSchema(
    name='prices_daily',
    description='Daily OHLCV prices from Stooq',
    columns=[
        ColumnSpec('symbol',
                   'str',
                   nullable=False,
                   description='Ticker symbol (e.g., AAPL.US)'),
        ColumnSpec('date',
                   'datetime64[ns]',
                   nullable=False,
                   description='Trading date'),
        ColumnSpec('open', 'float64', nullable=False, description='Open price'),
        ColumnSpec('high', 'float64', nullable=False, description='High price'),
        ColumnSpec('low', 'float64', nullable=False, description='Low price'),
        ColumnSpec('close',
                   'float64',
                   nullable=False,
                   description='Close price'),
        ColumnSpec('volume',
                   'float64',
                   nullable=False,
                   description='Trading volume'),
    ],
    primary_key=['symbol', 'date'])

COMPANIES_SCHEMA = DatasetSchema(
    name='companies',
    description='Company master table with fiscal year end and filing metadata',
    columns=[
        ColumnSpec('cik10',
                   'str',
                   nullable=False,
                   description='CIK padded to 10 digits'),
        ColumnSpec('ticker',
                   'str',
                   nullable=False,
                   description='Primary ticker symbol'),
        ColumnSpec('title',
                   'str',
                   nullable=False,
                   description='Company legal name'),
        ColumnSpec('fye_mmdd',
                   'str',
                   nullable=False,
                   description='Fiscal year end (MMDD format)'),
        ColumnSpec('first_filing_date',
                   'datetime64[ns]',
                   nullable=True,
                   description='Date of first SEC filing'),
    ],
    primary_key=['cik10'])
