from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from valuation.data_loader import ValuationDataLoader


class TestValuationDataLoader:

  def test_initialization(self):
    """Default initialization."""
    loader = ValuationDataLoader()

    assert loader.gold_path == Path('data/gold/out/valuation_panel.parquet')
    assert loader.silver_dir == Path('data/silver/out')

  def test_custom_paths(self):
    """Custom paths initialization."""
    gold_path = Path('custom/gold/panel.parquet')
    silver_dir = Path('custom/silver')

    loader = ValuationDataLoader(gold_path=gold_path, silver_dir=silver_dir)

    assert loader.gold_path == gold_path
    assert loader.silver_dir == silver_dir

  def test_load_panel_caching(self):
    """Panel data is cached after first load."""
    loader = ValuationDataLoader()

    # Mock file operations
    mock_panel = pd.DataFrame({
        'ticker': ['AAPL', 'GOOGL'],
        'end': ['2024-12-31', '2024-12-31'],
        'filed': ['2025-02-14', '2025-02-14'],
        'shares_q': [100, 200],
    })

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet',
                      return_value=mock_panel) as mock_read:
        # First call
        panel1 = loader.load_panel()
        assert mock_read.call_count == 1

        # Second call (should use cache, no additional file read)
        panel2 = loader.load_panel()
        assert mock_read.call_count == 1  # Still 1, not 2

        # Should return the same data
        pd.testing.assert_frame_equal(panel1, panel2)

  def test_load_prices_caching(self):
    """Price data is cached after first load."""
    loader = ValuationDataLoader()

    mock_prices = pd.DataFrame({
        'symbol': ['AAPL.US', 'GOOGL.US'],
        'date': ['2024-12-31', '2024-12-31'],
        'close': [180.0, 140.0],
    })

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet',
                      return_value=mock_prices) as mock_read:
        # First call
        prices1 = loader.load_prices()
        assert mock_read.call_count == 1

        # Second call (should use cache)
        prices2 = loader.load_prices()
        assert mock_read.call_count == 1  # Still 1, not 2

        # Should return the same data
        pd.testing.assert_frame_equal(prices1, prices2)

  def test_clear_cache(self):
    """Cache can be cleared and data reloaded."""
    loader = ValuationDataLoader()

    mock_panel = pd.DataFrame({
        'ticker': ['AAPL'],
        'end': ['2024-12-31'],
        'filed': ['2025-02-14'],
        'shares_q': [100],
    })

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet',
                      return_value=mock_panel) as mock_read:
        # Load once
        loader.load_panel()
        assert mock_read.call_count == 1

        # Clear cache
        loader.clear_cache()

        # Load again (should read file again)
        loader.load_panel()
        assert mock_read.call_count == 2

  def test_panel_not_found(self):
    """FileNotFoundError when Gold panel missing."""
    loader = ValuationDataLoader(gold_path=Path('nonexistent.parquet'))

    with pytest.raises(FileNotFoundError, match='Gold panel not found'):
      loader.load_panel()

  def test_prices_not_found(self):
    """FileNotFoundError when price data missing."""
    loader = ValuationDataLoader(silver_dir=Path('nonexistent'))

    with pytest.raises(FileNotFoundError, match='Price data not found'):
      loader.load_prices()

  def test_split_adjustment_2_for_1(self):
    """2-for-1 stock split is adjusted correctly."""
    # Stock split occurs: shares more than double from 100 to 210
    mock_panel = pd.DataFrame({
        'ticker': ['TEST', 'TEST', 'TEST'],
        'end': pd.to_datetime(['2023-12-31', '2024-03-31', '2024-06-30']),
        'filed': pd.to_datetime(['2024-02-14', '2024-05-14', '2024-08-14']),
        'shares_q': [100.0, 210.0, 210.0],  # Split at 2024-03-31
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_panel):
        panel = loader.load_panel()

        # All historical shares should be adjusted
        # shares_ratio = 210/100 = 2.1 (> 2, detected as split)
        test_data = panel[panel['ticker'] == 'TEST'].sort_values('end')
        assert len(test_data) == 3
        # All shares should be on the same basis (210)
        assert test_data.iloc[0]['shares_q'] == pytest.approx(210.0)
        assert test_data.iloc[1]['shares_q'] == pytest.approx(210.0)
        assert test_data.iloc[2]['shares_q'] == pytest.approx(210.0)

  def test_split_adjustment_reverse_split(self):
    """1-for-2 reverse split is adjusted correctly."""
    mock_panel = pd.DataFrame({
        'ticker': ['TEST', 'TEST', 'TEST'],
        'end': pd.to_datetime(['2023-12-31', '2024-03-31', '2024-06-30']),
        'filed': pd.to_datetime(['2024-02-14', '2024-05-14', '2024-08-14']),
        'shares_q': [200.0, 90.0, 90.0],  # Reverse split at 2024-03-31
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_panel):
        panel = loader.load_panel()

        # All shares should be on the same basis (90)
        # shares_ratio = 90/200 = 0.45 (< 0.5, detected as reverse split)
        test_data = panel[panel['ticker'] == 'TEST'].sort_values('end')
        assert len(test_data) == 3
        assert test_data.iloc[0]['shares_q'] == pytest.approx(90.0)
        assert test_data.iloc[1]['shares_q'] == pytest.approx(90.0)
        assert test_data.iloc[2]['shares_q'] == pytest.approx(90.0)

  def test_split_adjustment_multiple_tickers(self):
    """Handle multiple tickers independently."""
    mock_panel = pd.DataFrame({
        'ticker': ['AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
        'end':
            pd.to_datetime([
                '2023-12-31',
                '2024-03-31',  # AAPL
                '2023-12-31',
                '2024-03-31',  # GOOGL
            ]),
        'filed':
            pd.to_datetime([
                '2024-02-14',
                '2024-05-14',  # AAPL
                '2024-02-14',
                '2024-05-14',  # GOOGL
            ]),
        'shares_q': [
            100.0,
            220.0,  # AAPL: 2.2-for-1 split
            300.0,
            300.0,  # GOOGL: no split
        ],
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_panel):
        panel = loader.load_panel()

        # AAPL historical shares adjusted
        # shares_ratio = 220/100 = 2.2 (> 2, detected as split)
        aapl_data = panel[panel['ticker'] == 'AAPL'].sort_values('end')
        assert len(aapl_data) == 2
        assert aapl_data.iloc[0]['shares_q'] == pytest.approx(220.0)
        assert aapl_data.iloc[1]['shares_q'] == pytest.approx(220.0)

        # GOOGL unchanged
        googl_data = panel[panel['ticker'] == 'GOOGL'].sort_values('end')
        assert len(googl_data) == 2
        assert googl_data.iloc[0]['shares_q'] == pytest.approx(300.0)
        assert googl_data.iloc[1]['shares_q'] == pytest.approx(300.0)

  def test_no_split_adjustment_small_changes(self):
    """No adjustment when shares change is small (buybacks)."""
    mock_panel = pd.DataFrame({
        'ticker': ['TEST', 'TEST', 'TEST'],
        'end': pd.to_datetime(['2023-12-31', '2024-03-31', '2024-06-30']),
        'filed': pd.to_datetime(['2024-02-14', '2024-05-14', '2024-08-14']),
        'shares_q': [100.0, 101.0, 99.0],  # Small buybacks, no split
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_panel):
        panel = loader.load_panel()

        # Shares should remain unchanged (no split detected)
        test_data = panel[panel['ticker'] == 'TEST'].sort_values('end')
        assert len(test_data) == 3
        assert test_data.iloc[0]['shares_q'] == pytest.approx(100.0)
        assert test_data.iloc[1]['shares_q'] == pytest.approx(101.0)
        assert test_data.iloc[2]['shares_q'] == pytest.approx(99.0)

  def test_missing_shares_column(self):
    """Handle DataFrames without shares_q column."""
    mock_panel = pd.DataFrame({
        'ticker': ['TEST'],
        'end': pd.to_datetime(['2024-12-31']),
        'filed': pd.to_datetime(['2025-02-14']),
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_panel):
        panel = loader.load_panel()

        # Should return without error
        assert len(panel) == 1
        assert 'shares_q' not in panel.columns

  def test_load_panel_datetime_conversion(self):
    """Dates are properly converted to datetime."""
    mock_panel = pd.DataFrame({
        'ticker': ['AAPL'],
        'end': ['2024-12-31'],  # String date
        'filed': ['2025-02-14'],  # String date
        'shares_q': [100],
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_panel):
        panel = loader.load_panel()

        # Dates should be converted to datetime
        assert pd.api.types.is_datetime64_any_dtype(panel['end'])
        assert pd.api.types.is_datetime64_any_dtype(panel['filed'])

  def test_load_prices_datetime_conversion(self):
    """Price dates are properly converted to datetime."""
    mock_prices = pd.DataFrame({
        'symbol': ['AAPL.US'],
        'date': ['2024-12-31'],  # String date
        'close': [180.0],
    })

    loader = ValuationDataLoader()

    with mock.patch.object(Path, 'exists', return_value=True):
      with mock.patch('pandas.read_parquet', return_value=mock_prices):
        prices = loader.load_prices()

        # Dates should be converted to datetime
        assert pd.api.types.is_datetime64_any_dtype(prices['date'])
