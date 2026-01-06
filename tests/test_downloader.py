"""Tests for downloader module."""

import pytest
from downloader import CNPJ_FILE_PATTERNS


class TestFilePatternMatching:
    """Test file pattern matching functionality."""
    
    def test_simples_pattern_in_filename(self):
        """Test that SIMPLES pattern is found in typical filename."""
        filename = "F.K03200$W.SIMPLES.CSV.D51213"
        result = "SIMPLES" in filename
        assert result is True
    
    def test_simples_pattern_case_insensitive(self):
        """Test that SIMPLES pattern works with different cases."""
        filename_lower = "f.k03200$w.simples.csv.d51213"
        filename_upper = "F.K03200$W.SIMPLES.CSV.D51213"
        
        # Test direct string matching (case sensitive)
        assert "SIMPLES" in filename_upper
        assert "SIMPLES" not in filename_lower
        
        # Test case insensitive matching
        assert "SIMPLES" in filename_lower.upper()
        assert "SIMPLES" in filename_upper.upper()
    
    def test_cnpj_file_patterns_contains_simples(self):
        """Test that CNPJ_FILE_PATTERNS contains SIMPLES pattern."""
        assert "SIMPLES" in CNPJ_FILE_PATTERNS
    
    def test_cnpj_file_patterns_matching(self):
        """Test that filename extraction logic works with CNPJ_FILE_PATTERNS."""
        filename = "F.K03200$W.SIMPLES.CSV.D51213"
        filename_upper = filename.upper()
        
        # This simulates the logic from _download_and_extract method
        is_cnpj_file = any(
            pattern in filename_upper for pattern in CNPJ_FILE_PATTERNS
        )
        assert is_cnpj_file is True
    
    def test_other_patterns_matching(self):
        """Test that other CNPJ file patterns work correctly."""
        test_cases = [
            ("EMPRECSV", "EMPRECSV"),
            ("ESTABELE", "ESTABELE"), 
            ("SOCIOCSV", "SOCIOCSV"),
            ("CNAECSV", "CNAECSV"),
        ]
        
        for filename_part, expected_pattern in test_cases:
            filename_upper = f"F.K03200$W.{filename_part}.D51213"
            is_cnpj_file = any(
                pattern in filename_upper for pattern in CNPJ_FILE_PATTERNS
            )
            assert is_cnpj_file is True, f"Pattern {expected_pattern} should match {filename_part}"
    
    def test_non_matching_patterns(self):
        """Test that non-CNPJ files are not matched."""
        non_cnpj_files = [
            "README.txt",
            "config.json",
            "F.K03200$W.RANDOM.CSV.D51213",
            "some_other_file.csv"
        ]
        
        for filename in non_cnpj_files:
            filename_upper = filename.upper()
            is_cnpj_file = any(
                pattern in filename_upper for pattern in CNPJ_FILE_PATTERNS
            )
            assert is_cnpj_file is False, f"File {filename} should not be matched"