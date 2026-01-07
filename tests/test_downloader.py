"""Tests for downloader module."""

from downloader import CNPJ_FILE_PATTERNS


class TestFilePatternMatching:
    """Test file pattern matching functionality used in the project."""

    def test_cnpj_file_patterns_contains_simples(self):
        """Test that CNPJ_FILE_PATTERNS contains SIMPLES pattern."""
        assert "SIMPLES" in CNPJ_FILE_PATTERNS

    def test_cnpj_file_patterns_matching_logic(self):
        """Test the actual pattern matching logic used in _download_and_extract method."""
        test_cases = [
            ("F.K03200$W.SIMPLES.CSV.D51213", True),
            ("F.K03200$W.EMPRECSV.D51213", True),
            ("F.K03200$W.ESTABELE.D51213", True),
            ("F.K03200$W.SOCIOCSV.D51213", True),
            ("F.K03200$W.CNAECSV.D51213", True),
            ("README.txt", False),
            ("config.json", False),
            ("F.K03200$W.RANDOM.CSV.D51213", False),
            ("some_other_file.csv", False),
        ]

        for filename, expected in test_cases:
            filename_upper = filename.upper()
            # This replicates the logic from _download_and_extract method
            is_cnpj_file = any(pattern in filename_upper for pattern in CNPJ_FILE_PATTERNS)
            assert is_cnpj_file == expected, f"File {filename} matching should be {expected}"
