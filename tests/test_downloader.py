"""Tests for downloader module."""

import logging
import zipfile
from unittest.mock import MagicMock, patch

import pytest
import requests

from config import Config
from downloader import (
    CNPJ_FILE_PATTERNS,
    AdaptiveDownloadConcurrency,
    Downloader,
    DownloadIncompleteError,
    DownloadStalledError,
)


def _webdav_xml(entries: list[str]) -> bytes:
    """Build a minimal WebDAV PROPFIND XML response."""
    responses = ""
    for href in entries:
        responses += (
            f"<d:response><d:href>{href}</d:href>"
            "<d:propstat><d:prop/>"
            "<d:status>HTTP/1.1 200 OK</d:status>"
            "</d:propstat></d:response>"
        )
    return (f'<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">{responses}</d:multistatus>').encode()


@pytest.fixture
def config(tmp_path):
    """Create a test config with temp directory."""
    return Config(
        database_url="postgresql://test",
        temp_dir=str(tmp_path),
        retry_attempts=3,
        retry_delay=0,  # No delay in tests
        connect_timeout=5,
        read_timeout=10,
        keep_files=False,
    )


@pytest.fixture
def downloader(config):
    """Create a downloader instance."""
    return Downloader(config)


class _ScriptedResponse:
    def __init__(self, chunks: list[bytes | Exception], headers: dict[str, str], status_code: int = 200):
        self._chunks = chunks
        self.headers = headers
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size: int):
        for chunk in self._chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk


class _ScriptedGet:
    def __init__(self, responses: list[_ScriptedResponse | Exception]):
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def __call__(self, url: str, **kwargs):
        self.calls.append({"url": url, "headers": kwargs.get("headers") or {}, "timeout": kwargs.get("timeout")})
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _FakeClock:
    def __init__(self, times: list[float]):
        self._times = iter(times)
        self.current = 0.0

    def __call__(self) -> float:
        try:
            self.current = next(self._times)
        except StopIteration:
            pass
        return self.current


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


class TestGetAvailableDirectories:
    """Test WebDAV directory listing functionality."""

    def test_parses_directory_list(self, downloader):
        """Test that directory entries are correctly parsed from WebDAV XML."""
        xml = _webdav_xml(
            [
                "/public.php/webdav/",
                "/public.php/webdav/2024-01/",
                "/public.php/webdav/2024-02/",
                "/public.php/webdav/2024-03/",
            ]
        )
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock(content=xml, status_code=207)
            mock_req.return_value.raise_for_status = MagicMock()

            result = downloader.get_available_directories()

            assert result == ["2024-01", "2024-02", "2024-03"]

    def test_propfind_uses_metadata_read_timeout(self, downloader, config):
        """Discovery calls should keep the longer metadata read timeout."""
        config.stall_timeout = 2
        xml = _webdav_xml(["/public.php/webdav/", "/public.php/webdav/2024-03/"])
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock(content=xml, status_code=207)
            mock_req.return_value.raise_for_status = MagicMock()

            downloader.get_available_directories()

            assert mock_req.call_args.kwargs["timeout"] == (config.connect_timeout, config.read_timeout)

    def test_raises_on_network_error(self, downloader):
        """Test that network errors are propagated."""
        with patch("requests.request") as mock_req:
            mock_req.side_effect = requests.exceptions.ConnectionError("Network error")

            with pytest.raises(requests.exceptions.ConnectionError):
                downloader.get_available_directories()

    def test_raises_on_empty_response(self, downloader):
        """Test that empty listing raises ValueError."""
        xml = _webdav_xml(["/public.php/webdav/"])
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock(content=xml, status_code=207)
            mock_req.return_value.raise_for_status = MagicMock()

            with pytest.raises(ValueError, match="No data directories found"):
                downloader.get_available_directories()

    def test_raises_on_http_error(self, downloader):
        """Test that HTTP errors (404, 500) are propagated."""
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock()
            mock_req.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

            with pytest.raises(requests.exceptions.HTTPError):
                downloader.get_available_directories()


class TestGetLatestDirectory:
    """Test latest directory selection."""

    def test_returns_last_sorted_directory(self, downloader):
        """Test that the latest (last sorted) directory is returned."""
        with patch.object(downloader, "get_available_directories") as mock_dirs:
            mock_dirs.return_value = ["2024-01", "2024-02", "2024-03"]

            result = downloader.get_latest_directory()

            assert result == "2024-03"


class TestGetDirectoryFiles:
    """Test file listing from directory."""

    def test_parses_zip_files(self, downloader):
        """Test that ZIP file entries are correctly parsed from WebDAV XML."""
        xml = _webdav_xml(
            [
                "/public.php/webdav/2024-03/",
                "/public.php/webdav/2024-03/Empresas0.zip",
                "/public.php/webdav/2024-03/Empresas1.zip",
                "/public.php/webdav/2024-03/Cnaes.zip",
            ]
        )
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock(content=xml, status_code=207)
            mock_req.return_value.raise_for_status = MagicMock()

            result = downloader.get_directory_files("2024-03")

            assert "Empresas0.zip" in result
            assert "Cnaes.zip" in result

    def test_raises_on_http_error(self, downloader):
        """Test that HTTP errors are propagated."""
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock()
            mock_req.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

            with pytest.raises(requests.exceptions.HTTPError):
                downloader.get_directory_files("2024-03")


class TestDownloadAndExtract:
    """Test download and ZIP extraction functionality."""

    def test_retries_on_failure_then_succeeds(self, downloader, tmp_path):
        """Test that download retries on failure and succeeds on later attempt."""
        # Create a valid ZIP with a CNPJ file
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})

        with patch("requests.get") as mock_get:
            # Fail twice, succeed on third
            mock_response = MagicMock()
            mock_response.headers = {"content-length": str(len(zip_content))}
            mock_response.iter_content = MagicMock(return_value=[zip_content])
            mock_response.raise_for_status = MagicMock()

            mock_get.side_effect = [
                requests.exceptions.Timeout("Timeout 1"),
                requests.exceptions.Timeout("Timeout 2"),
                mock_response,
            ]

            result = downloader._download_and_extract("2024-03", "Cnaes.zip")

            assert len(result) == 1
            assert "CNAECSV" in result[0].name

    def test_raises_after_max_retries(self, downloader):
        """Test that exception is raised after all retries exhausted."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Timeout")

            with pytest.raises(requests.exceptions.Timeout):
                downloader._download_and_extract("2024-03", "Cnaes.zip")

            # Should have tried 3 times (retry_attempts=3)
            assert mock_get.call_count == 3

    def test_handles_corrupt_zip(self, downloader, tmp_path):
        """Test that corrupt ZIP files raise appropriate error."""
        corrupt_content = b"not a zip file"

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.headers = {"content-length": str(len(corrupt_content))}
            mock_response.iter_content = MagicMock(return_value=[corrupt_content])
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            with pytest.raises(zipfile.BadZipFile):
                downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_extracts_only_cnpj_files(self, downloader, tmp_path):
        """Test that only CNPJ pattern files are extracted from ZIP."""
        zip_content = _create_test_zip(
            tmp_path,
            {
                "CNAECSV.D51213": "data",
                "README.txt": "ignore this",
                "ESTABELE.D51213": "more data",
            },
        )

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.headers = {"content-length": str(len(zip_content))}
            mock_response.iter_content = MagicMock(return_value=[zip_content])
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            result = downloader._download_and_extract("2024-03", "Test.zip")

            # Should extract CNAECSV and ESTABELE, but not README.txt
            assert len(result) == 2
            names = [r.name for r in result]
            assert "CNAECSV.D51213" in names
            assert "ESTABELE.D51213" in names
            assert "README.txt" not in names

    def test_download_stream_uses_stall_timeout(self, downloader, config, tmp_path):
        """Streaming data requests should use stall_timeout as the read timeout."""
        config.stall_timeout = 7
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.headers = {"content-length": str(len(zip_content))}
            mock_response.iter_content = MagicMock(return_value=[zip_content])
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            downloader._download_and_extract("2024-03", "Cnaes.zip")

            assert mock_get.call_args.kwargs["timeout"] == (config.connect_timeout, config.stall_timeout)

    def test_resumes_after_timeout_with_range_header(self, downloader, tmp_path, monkeypatch, caplog):
        """A stalled stream should retry from the bytes already saved in .part."""
        downloader.config.keep_files = True
        downloader.config.stall_timeout = 7
        caplog.set_level(logging.WARNING, logger="downloader")
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        split_at = len(zip_content) // 2
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[:split_at], requests.exceptions.Timeout("stalled stream")],
                    headers={"content-length": str(len(zip_content))},
                ),
                _ScriptedResponse(
                    chunks=[zip_content[split_at:]],
                    headers={
                        "content-range": f"bytes {split_at}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - split_at),
                    },
                    status_code=206,
                ),
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert scripted_get.calls[0]["headers"] == {"Accept-Encoding": "identity"}
        assert scripted_get.calls[1]["headers"] == {"Accept-Encoding": "identity", "Range": f"bytes={split_at}-"}
        assert f"Cnaes.zip stalled: no bytes for 7s, resuming from offset {split_at}" in caplog.text

    def test_stream_timeout_error_is_distinct_download_failure(self, downloader, tmp_path, monkeypatch):
        """A final stalled attempt should surface a distinct resumable-download error."""
        downloader.config.retry_attempts = 1
        downloader.config.stall_timeout = 7
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        split_at = len(zip_content) // 2
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[:split_at], requests.exceptions.Timeout("stalled stream")],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadStalledError, match=f"resuming from offset {split_at}"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_data_get_read_timeout_is_distinct_download_failure(self, downloader, monkeypatch):
        """A data request read timeout before the body is still a resumable stall."""
        downloader.config.retry_attempts = 1
        downloader.config.stall_timeout = 7
        monkeypatch.setattr(requests, "get", MagicMock(side_effect=requests.exceptions.ReadTimeout("slow stream")))

        with pytest.raises(DownloadStalledError, match="resuming from offset 0"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_resume_appends_to_existing_partial_file(self, downloader, tmp_path, monkeypatch):
        """A leftover .part file is appended to instead of redownloading from byte zero."""
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        split_at = len(zip_content) // 2
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(zip_content[:split_at])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[split_at:]],
                    headers={
                        "content-range": f"bytes {split_at}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - split_at),
                    },
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert scripted_get.calls[0]["headers"] == {"Accept-Encoding": "identity", "Range": f"bytes={split_at}-"}
        assert (tmp_path / "Cnaes.zip").read_bytes() == zip_content

    def test_server_ignoring_range_discards_partial_and_restarts(self, downloader, tmp_path, monkeypatch):
        """A 200 response to a Range request is a clean restart, not an append."""
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(b"stale-part")
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content],
                    headers={"content-length": str(len(zip_content))},
                    status_code=200,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert scripted_get.calls[0]["headers"] == {
            "Accept-Encoding": "identity",
            "Range": f"bytes={len(b'stale-part')}-",
        }
        assert (tmp_path / "Cnaes.zip").read_bytes() == zip_content

    def test_final_size_mismatch_raises_without_final_zip(self, downloader, tmp_path, monkeypatch):
        """A short final size is retried, then left as .part without publishing a bad ZIP."""
        downloader.config.keep_files = True
        downloader.config.retry_attempts = 2
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        first_split = len(zip_content) // 3
        second_split = first_split * 2
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[:first_split]],
                    headers={"content-length": str(len(zip_content))},
                ),
                _ScriptedResponse(
                    chunks=[zip_content[first_split:second_split]],
                    headers={
                        "content-range": f"bytes {first_split}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - first_split),
                    },
                    status_code=206,
                ),
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(RuntimeError, match="Incomplete download"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert not (tmp_path / "Cnaes.zip").exists()
        assert (tmp_path / "Cnaes.zip.2024-03.part").read_bytes() == zip_content[:second_split]

    def test_corrupt_zip_is_retried_then_raises(self, downloader, tmp_path, monkeypatch):
        """Downloaded bytes must be a readable ZIP before extraction starts."""
        downloader.config.keep_files = True
        downloader.config.retry_attempts = 2
        corrupt_content = b"not a zip file"
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[corrupt_content],
                    headers={"content-length": str(len(corrupt_content))},
                ),
                _ScriptedResponse(
                    chunks=[corrupt_content],
                    headers={"content-length": str(len(corrupt_content))},
                ),
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(zipfile.BadZipFile):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(scripted_get.calls) == 2
        assert not (tmp_path / "Cnaes.zip").exists()
        assert not (tmp_path / "Cnaes.zip.2024-03.part").exists()


class TestDownloadFiles:
    """Test the main download_files orchestration."""

    def test_returns_empty_for_empty_list(self, downloader):
        """Test that empty file list returns immediately."""
        result = list(downloader.download_files("2024-03", []))

        assert result == []

    def test_reference_download_failure_propagates(self, downloader, tmp_path):
        """A reference file download failure should propagate to the caller."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Timeout")

            with pytest.raises(requests.exceptions.Timeout):
                list(downloader.download_files("2024-03", ["Cnaes.zip"]))


class TestAdaptiveDownloadConcurrency:
    """Test adaptive concurrency degradation decisions."""

    def test_degrades_from_four_to_two_to_one_at_threshold_crossings(self, caplog):
        caplog.set_level(logging.WARNING, logger="downloader")
        adaptive_concurrency = AdaptiveDownloadConcurrency(initial_concurrency=4, stall_degrade_threshold=2)

        assert adaptive_concurrency.current_concurrency == 4
        assert adaptive_concurrency.record_stall() is None
        assert adaptive_concurrency.current_concurrency == 4

        first_degradation = adaptive_concurrency.record_stall()
        assert first_degradation is not None
        assert first_degradation.previous_concurrency == 4
        assert first_degradation.new_concurrency == 2
        assert adaptive_concurrency.current_concurrency == 2

        assert adaptive_concurrency.record_stall() is None
        assert adaptive_concurrency.current_concurrency == 2

        second_degradation = adaptive_concurrency.record_stall()
        assert second_degradation is not None
        assert second_degradation.previous_concurrency == 2
        assert second_degradation.new_concurrency == 1
        assert adaptive_concurrency.current_concurrency == 1

        assert "2 stalls at concurrency 4, degrading to 2 for the rest of the run" in caplog.text
        assert "4 stalls at concurrency 2, degrading to 1 for the rest of the run" in caplog.text

    def test_degradation_never_scales_back_up(self):
        adaptive_concurrency = AdaptiveDownloadConcurrency(initial_concurrency=4, stall_degrade_threshold=1)

        for _ in range(5):
            adaptive_concurrency.record_stall()

        assert adaptive_concurrency.current_concurrency == 1
        assert adaptive_concurrency.stall_count == 5

    def test_stalls_below_threshold_leave_concurrency_unchanged(self, caplog):
        caplog.set_level(logging.WARNING, logger="downloader")
        adaptive_concurrency = AdaptiveDownloadConcurrency(initial_concurrency=4, stall_degrade_threshold=3)

        adaptive_concurrency.record_stall()
        adaptive_concurrency.record_stall()

        assert adaptive_concurrency.current_concurrency == 4
        assert "degrading" not in caplog.text


class TestAdaptiveDownloadIntegration:
    """Test adaptive degradation through the resumable download path."""

    def test_stalled_file_resumes_and_completes_after_degrading_to_one(self, downloader, tmp_path, monkeypatch, caplog):
        downloader.config.download_workers = 4
        downloader.config.stall_degrade_threshold = 1
        downloader.config.retry_attempts = 3
        downloader.config.retry_delay = 0
        downloader.config.keep_files = True
        monkeypatch.setenv("TQDM_DISABLE", "1")
        caplog.set_level(logging.WARNING, logger="downloader")
        zip_content = _create_test_zip(tmp_path, {"EMPRECSV.D51213": "0111301;Test"})
        first_split = len(zip_content) // 3
        second_split = first_split * 2
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[:first_split], requests.exceptions.Timeout("stalled stream")],
                    headers={"content-length": str(len(zip_content))},
                ),
                _ScriptedResponse(
                    chunks=[zip_content[first_split:second_split], requests.exceptions.Timeout("stalled stream")],
                    headers={
                        "content-range": f"bytes {first_split}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - first_split),
                    },
                    status_code=206,
                ),
                _ScriptedResponse(
                    chunks=[zip_content[second_split:]],
                    headers={
                        "content-range": f"bytes {second_split}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - second_split),
                    },
                    status_code=206,
                ),
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = list(downloader.download_files("2024-03", ["Empresas0.zip"]))

        assert len(result) == 1
        assert result[0][1] == "Empresas0.zip"
        assert scripted_get.calls[0]["headers"] == {"Accept-Encoding": "identity"}
        assert scripted_get.calls[1]["headers"] == {"Accept-Encoding": "identity", "Range": f"bytes={first_split}-"}
        assert scripted_get.calls[2]["headers"] == {"Accept-Encoding": "identity", "Range": f"bytes={second_split}-"}
        assert "1 stalls at concurrency 4, degrading to 2 for the rest of the run" in caplog.text
        assert "2 stalls at concurrency 2, degrading to 1 for the rest of the run" in caplog.text


class TestCleanup:
    """Test cleanup functionality."""

    def test_removes_temp_files(self, config, tmp_path):
        """Test that cleanup removes temporary files."""
        # Create some temp files
        (tmp_path / "file1.csv").write_text("data")
        (tmp_path / "file2.zip").write_bytes(b"data")

        downloader = Downloader(config)
        downloader.cleanup()

        assert len(list(tmp_path.glob("*"))) == 0

    def test_skips_cleanup_when_keep_files(self, tmp_path):
        """Test that cleanup is skipped when keep_files is True."""
        config = Config(
            database_url="postgresql://test",
            temp_dir=str(tmp_path),
            keep_files=True,
        )
        (tmp_path / "file1.csv").write_text("data")

        downloader = Downloader(config)
        downloader.cleanup()

        # File should still exist
        assert (tmp_path / "file1.csv").exists()


class TestCachedDownload:
    """Test caching behavior when keep_files is enabled."""

    def test_uses_cached_zip_when_valid(self, tmp_path):
        """Test that existing valid ZIP is reused instead of downloading."""
        config = Config(
            database_url="postgresql://test",
            temp_dir=str(tmp_path),
            keep_files=True,
        )

        # Pre-create a valid ZIP file
        zip_path = tmp_path / "Cnaes.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("CNAECSV.D51213", "0111301;Test")

        downloader = Downloader(config)

        with patch("requests.get") as mock_get:
            result = downloader._download_and_extract("2024-03", "Cnaes.zip")

            # Should not have made any HTTP requests
            mock_get.assert_not_called()
            assert len(result) == 1


class TestProgressLogging:
    """Test periodic progress logs when tqdm output is disabled."""

    def test_logs_progress_at_configured_cadence(self, downloader, tmp_path, monkeypatch, caplog):
        downloader.config.progress_log_interval = 5
        downloader.config.stall_timeout = 60
        monkeypatch.setenv("TQDM_DISABLE", "1")
        monkeypatch.setattr("downloader.monotonic", _FakeClock([0.0, 2.0, 5.0, 8.0, 10.0]))
        caplog.set_level(logging.INFO, logger="downloader")
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "x" * 200})
        first = len(zip_content) // 4
        second = len(zip_content) // 2
        third = len(zip_content) * 3 // 4
        chunks = [
            zip_content[:first],
            zip_content[first:second],
            zip_content[second:third],
            zip_content[third:],
        ]
        scripted_get = _ScriptedGet(
            [_ScriptedResponse(chunks=chunks, headers={"content-length": str(len(zip_content))})]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader._download_and_extract("2024-03", "Cnaes.zip")

        progress_logs = [record.message for record in caplog.records if "progress:" in record.message]
        first_logged_bytes = len(chunks[0]) + len(chunks[1])
        first_rate = first_logged_bytes / 5
        first_eta = (len(zip_content) - first_logged_bytes) / first_rate
        final_rate = (len(zip_content) - first_logged_bytes) / 5
        assert len(progress_logs) == 2
        assert (
            f"Cnaes.zip progress: {first_logged_bytes}/{len(zip_content)} bytes "
            f"({first_logged_bytes / len(zip_content) * 100:.1f}%), {first_rate:.1f} B/s, "
            f"ETA {first_eta:.1f}s"
        ) in progress_logs
        assert (
            f"Cnaes.zip progress: {len(zip_content)}/{len(zip_content)} bytes (100.0%), {final_rate:.1f} B/s, ETA 0.0s"
        ) in progress_logs

    def test_progress_logging_is_silent_when_tqdm_is_enabled(self, downloader, tmp_path, monkeypatch, caplog):
        downloader.config.progress_log_interval = 1
        downloader.config.stall_timeout = 60
        monkeypatch.delenv("TQDM_DISABLE", raising=False)
        monkeypatch.setattr("downloader.monotonic", _FakeClock([0.0, 2.0, 4.0, 6.0]))
        caplog.set_level(logging.INFO, logger="downloader")
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "x" * 200})
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[:10], zip_content[10:]], headers={"content-length": str(len(zip_content))}
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert not [record for record in caplog.records if "progress:" in record.message]

    def test_progress_logging_is_silent_when_interval_is_zero(self, downloader, tmp_path, monkeypatch, caplog):
        downloader.config.progress_log_interval = 0
        downloader.config.stall_timeout = 60
        monkeypatch.setenv("TQDM_DISABLE", "1")
        monkeypatch.setattr("downloader.monotonic", _FakeClock([0.0, 2.0, 4.0, 6.0]))
        caplog.set_level(logging.INFO, logger="downloader")
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "x" * 200})
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[:10], zip_content[10:]], headers={"content-length": str(len(zip_content))}
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert not [record for record in caplog.records if "progress:" in record.message]


def _create_test_zip(tmp_path, files: dict) -> bytes:
    """Helper to create a ZIP file with given contents."""
    zip_path = tmp_path / "temp_test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)

    content = zip_path.read_bytes()
    zip_path.unlink()
    return content


class TestResumeEdgeCases:
    """Branch coverage for the resume protocol's error and finalize paths."""

    def _zip(self, tmp_path):
        return _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})

    def test_416_with_matching_total_finalizes_partial(self, downloader, tmp_path, monkeypatch):
        downloader.config.keep_files = True
        zip_content = self._zip(tmp_path)
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(zip_content)
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[],
                    headers={"content-range": f"bytes */{len(zip_content)}"},
                    status_code=416,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert not part_path.exists()
        assert (tmp_path / "Cnaes.zip").read_bytes() == zip_content

    def test_416_with_mismatched_total_discards_partial(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(zip_content[: len(zip_content) // 2])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[],
                    headers={"content-range": f"bytes */{len(zip_content)}"},
                    status_code=416,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="differs from remote size"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")
        assert not part_path.exists()

    def test_416_without_content_range_discards_partial(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[], headers={}, status_code=416)])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="did not report the remote size"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")
        assert not part_path.exists()

    def test_206_resuming_at_wrong_offset_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        offset = len(zip_content) // 2
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:offset])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[offset - 1 :]],
                    headers={"content-range": f"bytes {offset - 1}-{len(zip_content) - 1}/{len(zip_content)}"},
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="server resumed at byte"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_206_with_remote_smaller_than_partial_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        offset = len(zip_content)
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content)
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[],
                    headers={"content-range": f"bytes {offset}-{offset}/{offset - 1}"},
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="exceeds remote size"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_206_content_length_mismatching_range_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        offset = len(zip_content) // 2
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:offset])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[offset:]],
                    headers={
                        "content-range": f"bytes {offset}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": "1",
                    },
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Content-Length mismatch"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_206_with_invalid_content_range_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        offset = len(zip_content) // 2
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:offset])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[offset:]],
                    headers={"content-range": "bytes nonsense"},
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Invalid Content-Range"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_resume_with_unexpected_status_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[], headers={}, status_code=204)])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="server returned HTTP 204"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_missing_content_length_on_fresh_download_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[zip_content], headers={})])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Missing Content-Length"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_invalid_content_length_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[zip_content], headers={"content-length": "many"})])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Invalid Content-Length"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_overlong_body_discards_partial_and_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content],
                    headers={"content-length": str(len(zip_content) - 4)},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="expected"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")
        assert not (tmp_path / "Cnaes.zip").exists()

    def test_stale_partial_from_other_month_is_not_resumed(self, downloader, tmp_path, monkeypatch):
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        other_month_part = tmp_path / "Cnaes.zip.2024-02.part"
        other_month_part.write_bytes(b"stale bytes from another month")
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert "Range" not in scripted_get.calls[0]["headers"]
        assert other_month_part.read_bytes() == b"stale bytes from another month"

    def test_preexisting_final_zip_is_replaced(self, downloader, tmp_path, monkeypatch):
        downloader.config.keep_files = False
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        stale_final = tmp_path / "Cnaes.zip"
        stale_final.write_bytes(b"not the zip we want")
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    # An empty leading chunk exercises the keep-alive skip.
                    chunks=[b"", zip_content],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1

    def test_206_without_content_length_is_accepted(self, downloader, tmp_path, monkeypatch):
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        offset = len(zip_content) // 2
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:offset])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[zip_content[offset:]],
                    headers={"content-range": f"bytes {offset}-{len(zip_content) - 1}/{len(zip_content)}"},
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert (tmp_path / "Cnaes.zip").read_bytes() == zip_content

    def test_206_missing_content_range_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[], headers={}, status_code=206)])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Missing Content-Range"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_206_reversed_content_range_raises(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[],
                    headers={"content-range": "bytes 9-3/100"},
                    status_code=206,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Invalid Content-Range"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_416_with_malformed_content_range_discards_partial(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[],
                    headers={"content-range": "weird"},
                    status_code=416,
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="did not report the remote size"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")
        assert not part_path.exists()

    def test_zip_with_corrupt_member_crc_is_rejected(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = bytearray(_create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test-payload-long-enough"}))
        # Flip a byte inside the member payload: the archive structure stays
        # readable, but testzip() reports the CRC mismatch.
        marker = zip_content.find(b"0111301")
        zip_content[marker] ^= 0xFF
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[bytes(zip_content)],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(zipfile.BadZipFile, match="Corrupt ZIP member"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_corrupt_cached_zip_is_redownloaded(self, downloader, tmp_path, monkeypatch):
        downloader.config.keep_files = True
        zip_content = bytearray(_create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test-payload-long-enough"}))
        good_zip = bytes(zip_content)
        marker = zip_content.find(b"0111301")
        zip_content[marker] ^= 0xFF
        (tmp_path / "Cnaes.zip").write_bytes(bytes(zip_content))
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[good_zip],
                    headers={"content-length": str(len(good_zip))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert len(scripted_get.calls) == 1
        assert (tmp_path / "Cnaes.zip").read_bytes() == good_zip

    def test_cleanup_preserves_part_files(self, downloader, tmp_path):
        downloader.config.keep_files = False
        (tmp_path / "Cnaes.zip").write_bytes(b"done")
        (tmp_path / "Empresas0.zip.2024-03.part").write_bytes(b"resume me")

        downloader.cleanup()

        assert not (tmp_path / "Cnaes.zip").exists()
        assert (tmp_path / "Empresas0.zip.2024-03.part").exists()

    def test_empty_keepalive_chunks_past_stall_timeout_raise(self, downloader, tmp_path, monkeypatch):
        import downloader as downloader_module

        downloader.config.retry_attempts = 1
        downloader.config.stall_timeout = 30
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        clock = iter([0.0, 0.0, 100.0, 200.0, 300.0, 400.0, 500.0])
        monkeypatch.setattr(downloader_module, "monotonic", lambda: next(clock))
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[b"", b"", zip_content],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadStalledError, match="stalled: no bytes"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")

    def test_connection_error_read_timeout_maps_to_stall_and_resumes(self, downloader, tmp_path, monkeypatch):
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        split_at = len(zip_content) // 2
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[
                        zip_content[:split_at],
                        requests.exceptions.ConnectionError("Read timed out."),
                    ],
                    headers={"content-length": str(len(zip_content))},
                ),
                _ScriptedResponse(
                    chunks=[zip_content[split_at:]],
                    headers={
                        "content-range": f"bytes {split_at}-{len(zip_content) - 1}/{len(zip_content)}",
                    },
                    status_code=206,
                ),
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader._download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert scripted_get.calls[1]["headers"]["Range"] == f"bytes={split_at}-"

    def test_other_connection_errors_propagate_unmapped(self, downloader, tmp_path, monkeypatch):
        downloader.config.retry_attempts = 1
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[
                        zip_content[:4],
                        requests.exceptions.ConnectionError("Connection reset by peer"),
                    ],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(requests.exceptions.ConnectionError, match="reset by peer"):
            downloader._download_and_extract("2024-03", "Cnaes.zip")
