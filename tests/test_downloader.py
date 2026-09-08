"""Tests for downloader module."""

import logging
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import TypedDict
from unittest.mock import MagicMock, patch

import pytest
import requests
import urllib3.exceptions

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
def config(tmp_path: Path):
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
def downloader(config: Config):
    """Create a downloader instance."""
    return DownloadProbe(config)


class GetCall(TypedDict):
    url: str
    headers: dict[str, str]
    timeout: object


class DownloadProbe(Downloader):
    download_and_extract = Downloader._download_and_extract
    download_zip = Downloader._download_zip
    is_read_timeout = Downloader._is_read_timeout


class ConcurrencyProbe(AdaptiveDownloadConcurrency):
    @property
    def active_streams(self) -> int:
        return self._active_streams


class _ScriptedResponse:
    def __init__(self, chunks: Sequence[bytes | Exception], headers: dict[str, str], status_code: int = 200):
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
    def __init__(self, responses: Sequence[_ScriptedResponse | Exception]):
        self._responses = list(responses)
        self.calls: list[GetCall] = []

    def __call__(self, url: str, *, headers: dict[str, str] | None = None, timeout: object = None, **kwargs: object):
        self.calls.append({"url": url, "headers": headers or {}, "timeout": timeout})
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

    def test_cnpj_file_patterns_contains_simples(self) -> None:
        """Test that CNPJ_FILE_PATTERNS contains SIMPLES pattern."""
        assert "SIMPLES" in CNPJ_FILE_PATTERNS

    def test_cnpj_file_patterns_matching_logic(self) -> None:
        """Check the configured pattern list against representative filenames."""
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

    def test_parses_directory_list(self, downloader: DownloadProbe) -> None:
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

    def test_propfind_uses_metadata_read_timeout(self, downloader: DownloadProbe, config: Config) -> None:
        """Discovery calls should keep the longer metadata read timeout."""
        config.stall_timeout = 2
        xml = _webdav_xml(["/public.php/webdav/", "/public.php/webdav/2024-03/"])
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock(content=xml, status_code=207)
            mock_req.return_value.raise_for_status = MagicMock()

            downloader.get_available_directories()

            assert mock_req.call_args.kwargs["timeout"] == (config.connect_timeout, config.read_timeout)

    def test_raises_on_network_error(self, downloader: DownloadProbe) -> None:
        """Test that network errors are propagated."""
        with patch("requests.request") as mock_req:
            mock_req.side_effect = requests.exceptions.ConnectionError("Network error")

            with pytest.raises(requests.exceptions.ConnectionError):
                downloader.get_available_directories()

    def test_raises_on_empty_response(self, downloader: DownloadProbe) -> None:
        """Test that empty listing raises ValueError."""
        xml = _webdav_xml(["/public.php/webdav/"])
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock(content=xml, status_code=207)
            mock_req.return_value.raise_for_status = MagicMock()

            with pytest.raises(ValueError, match="No data directories found"):
                downloader.get_available_directories()

    def test_raises_on_http_error(self, downloader: DownloadProbe) -> None:
        """Test that HTTP errors (404, 500) are propagated."""
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock()
            mock_req.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

            with pytest.raises(requests.exceptions.HTTPError):
                downloader.get_available_directories()


class TestGetLatestDirectory:
    """Test latest directory selection."""

    def test_returns_last_sorted_directory(self, downloader: DownloadProbe) -> None:
        """Test that the latest (last sorted) directory is returned."""
        with patch.object(downloader, "get_available_directories") as mock_dirs:
            mock_dirs.return_value = ["2024-01", "2024-02", "2024-03"]

            result = downloader.get_latest_directory()

            assert result == "2024-03"


class TestGetDirectoryFiles:
    """Test file listing from directory."""

    def test_parses_zip_files(self, downloader: DownloadProbe) -> None:
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

    def test_raises_on_http_error(self, downloader: DownloadProbe) -> None:
        """Test that HTTP errors are propagated."""
        with patch("requests.request") as mock_req:
            mock_req.return_value = MagicMock()
            mock_req.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

            with pytest.raises(requests.exceptions.HTTPError):
                downloader.get_directory_files("2024-03")


class TestDownloadAndExtract:
    """Test download and ZIP extraction functionality."""

    def test_retries_on_failure_then_succeeds(self, downloader: DownloadProbe, tmp_path: Path) -> None:
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

            result = downloader.download_and_extract("2024-03", "Cnaes.zip")

            assert len(result) == 1
            assert "CNAECSV" in result[0].name

    def test_raises_after_max_retries(self, downloader: DownloadProbe) -> None:
        """Test that exception is raised after all retries exhausted."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Timeout")

            with pytest.raises(requests.exceptions.Timeout):
                downloader.download_and_extract("2024-03", "Cnaes.zip")

            # Should have tried 3 times (retry_attempts=3)
            assert mock_get.call_count == 3

    def test_handles_corrupt_zip(self, downloader: DownloadProbe, tmp_path: Path) -> None:
        """Test that corrupt ZIP files raise appropriate error."""
        corrupt_content = b"not a zip file"

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.headers = {"content-length": str(len(corrupt_content))}
            mock_response.iter_content = MagicMock(return_value=[corrupt_content])
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            with pytest.raises(zipfile.BadZipFile):
                downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_extracts_only_cnpj_files(self, downloader: DownloadProbe, tmp_path: Path) -> None:
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

            result = downloader.download_and_extract("2024-03", "Test.zip")

            # Should extract CNAECSV and ESTABELE, but not README.txt
            assert len(result) == 2
            names = [r.name for r in result]
            assert "CNAECSV.D51213" in names
            assert "ESTABELE.D51213" in names
            assert "README.txt" not in names

    def test_download_stream_uses_stall_timeout(
        self, downloader: DownloadProbe, config: Config, tmp_path: Path
    ) -> None:
        """Streaming data requests should use stall_timeout as the read timeout."""
        config.stall_timeout = 7
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.headers = {"content-length": str(len(zip_content))}
            mock_response.iter_content = MagicMock(return_value=[zip_content])
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            downloader.download_and_extract("2024-03", "Cnaes.zip")

            assert mock_get.call_args.kwargs["timeout"] == (config.connect_timeout, config.stall_timeout)

    def test_resumes_after_timeout_with_range_header(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
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

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert scripted_get.calls[0]["headers"] == {"Accept-Encoding": "identity"}
        assert scripted_get.calls[1]["headers"] == {"Accept-Encoding": "identity", "Range": f"bytes={split_at}-"}
        assert f"Cnaes.zip stalled: no bytes for 7s, resuming from offset {split_at}" in caplog.text

    def test_stream_timeout_error_is_distinct_download_failure(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
                ),
                # The retry resumes but receives nothing: no progress, budget exhausted.
                _ScriptedResponse(
                    chunks=[requests.exceptions.Timeout("stalled again")],
                    headers={
                        "content-range": f"bytes {split_at}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - split_at),
                    },
                    status_code=206,
                ),
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadStalledError, match=f"resuming from offset {split_at}"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_data_get_read_timeout_is_distinct_download_failure(
        self, downloader: DownloadProbe, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A data request read timeout before the body is still a resumable stall."""
        downloader.config.retry_attempts = 1
        downloader.config.stall_timeout = 7
        monkeypatch.setattr(requests, "get", MagicMock(side_effect=requests.exceptions.ReadTimeout("slow stream")))

        with pytest.raises(DownloadStalledError, match="resuming from offset 0"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_resume_appends_to_existing_partial_file(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert scripted_get.calls[0]["headers"] == {"Accept-Encoding": "identity", "Range": f"bytes={split_at}-"}
        assert (tmp_path / "2024-03.Cnaes.zip").read_bytes() == zip_content

    def test_server_ignoring_range_discards_partial_and_restarts(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert scripted_get.calls[0]["headers"] == {
            "Accept-Encoding": "identity",
            "Range": f"bytes={len(b'stale-part')}-",
        }
        assert (tmp_path / "2024-03.Cnaes.zip").read_bytes() == zip_content

    def test_final_size_mismatch_raises_without_final_zip(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            + [
                # Productive attempts reset the budget; only these two
                # empty-handed retries count against retry_attempts=2.
                _ScriptedResponse(
                    chunks=[],
                    headers={
                        "content-range": f"bytes {second_split}-{len(zip_content) - 1}/{len(zip_content)}",
                        "content-length": str(len(zip_content) - second_split),
                    },
                    status_code=206,
                )
                for _ in range(2)
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(RuntimeError, match="Incomplete download"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert not (tmp_path / "2024-03.Cnaes.zip").exists()
        assert (tmp_path / "Cnaes.zip.2024-03.part").read_bytes() == zip_content[:second_split]

    def test_corrupt_zip_is_retried_then_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(scripted_get.calls) == 2
        assert not (tmp_path / "2024-03.Cnaes.zip").exists()
        assert not (tmp_path / "Cnaes.zip.2024-03.part").exists()


class TestDownloadFiles:
    """Test the main download_files orchestration."""

    def test_returns_empty_for_empty_list(self, downloader: DownloadProbe) -> None:
        """Test that empty file list returns immediately."""
        result = list(downloader.download_files("2024-03", []))

        assert result == []

    def test_reference_download_failure_propagates(self, downloader: DownloadProbe, tmp_path: Path) -> None:
        """A reference file download failure should propagate to the caller."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Timeout")

            with pytest.raises(requests.exceptions.Timeout):
                list(downloader.download_files("2024-03", ["Cnaes.zip"]))


class TestAdaptiveDownloadConcurrency:
    """Test adaptive concurrency degradation decisions."""

    def test_degrades_from_four_to_two_to_one_at_threshold_crossings(self, caplog: pytest.LogCaptureFixture) -> None:
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

    def test_degradation_never_scales_back_up(self) -> None:
        adaptive_concurrency = AdaptiveDownloadConcurrency(initial_concurrency=4, stall_degrade_threshold=1)

        for _ in range(5):
            adaptive_concurrency.record_stall()

        assert adaptive_concurrency.current_concurrency == 1
        assert adaptive_concurrency.stall_count == 5

    def test_stalls_below_threshold_leave_concurrency_unchanged(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.WARNING, logger="downloader")
        adaptive_concurrency = AdaptiveDownloadConcurrency(initial_concurrency=4, stall_degrade_threshold=3)

        adaptive_concurrency.record_stall()
        adaptive_concurrency.record_stall()

        assert adaptive_concurrency.current_concurrency == 4
        assert "degrading" not in caplog.text


class TestAdaptiveDownloadIntegration:
    """Test adaptive degradation through the resumable download path."""

    def test_stalled_file_resumes_and_completes_after_degrading_to_one(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
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

    def test_removes_temp_files(self, config: Config, tmp_path: Path) -> None:
        archive = tmp_path / "2024-03.Cnaes.zip"
        with zipfile.ZipFile(archive, "w") as z:
            z.writestr("CNAECSV", "01;One\n")
        config.keep_files = True
        downloader = DownloadProbe(config)
        paths = downloader.download_file("2024-03", "Cnaes.zip")
        assert paths[0].exists()
        config.keep_files = False
        downloader.cleanup()
        downloader.cleanup()
        assert not archive.exists()
        assert not paths[0].exists()

    def test_skips_cleanup_when_keep_files(self, tmp_path: Path) -> None:
        """Test that cleanup is skipped when keep_files is True."""
        config = Config(
            database_url="postgresql://test",
            temp_dir=str(tmp_path),
            keep_files=True,
        )
        (tmp_path / "file1.csv").write_text("data")

        downloader = DownloadProbe(config)
        downloader.cleanup()

        assert (tmp_path / "file1.csv").exists()


class TestCachedDownload:
    """Test caching behavior when keep_files is enabled."""

    def test_uses_cached_zip_when_valid(self, tmp_path: Path) -> None:
        """Test that existing valid ZIP is reused instead of downloading."""
        config = Config(
            database_url="postgresql://test",
            temp_dir=str(tmp_path),
            keep_files=True,
        )

        # Pre-create a valid ZIP file
        zip_path = tmp_path / "2024-03.Cnaes.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("CNAECSV.D51213", "0111301;Test")

        downloader = DownloadProbe(config)

        with patch("requests.get") as mock_get:
            result = downloader.download_and_extract("2024-03", "Cnaes.zip")

            # Should not have made any HTTP requests
            mock_get.assert_not_called()
            assert len(result) == 1


class TestProgressLogging:
    """Test periodic progress logs when tqdm output is disabled."""

    def test_logs_progress_at_configured_cadence(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
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

        downloader.download_and_extract("2024-03", "Cnaes.zip")

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

    def test_progress_logging_is_silent_when_tqdm_is_enabled(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
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

        downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert not [record for record in caplog.records if "progress:" in record.message]

    def test_progress_logging_is_silent_when_interval_is_zero(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
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

        downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert not [record for record in caplog.records if "progress:" in record.message]


def _create_test_zip(tmp_path: Path, files: dict[str, str]) -> bytes:
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

    def _zip(self, tmp_path: Path):
        return _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})

    def test_416_with_matching_total_finalizes_partial(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert not part_path.exists()
        assert (tmp_path / "2024-03.Cnaes.zip").read_bytes() == zip_content

    def test_416_with_mismatched_total_discards_partial(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")
        assert not part_path.exists()

    def test_416_without_content_range_discards_partial(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        part_path = tmp_path / "Cnaes.zip.2024-03.part"
        part_path.write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[], headers={}, status_code=416)])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="did not report the remote size"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")
        assert not part_path.exists()

    def test_206_resuming_at_wrong_offset_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_206_with_remote_smaller_than_partial_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_206_content_length_mismatching_range_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_206_with_invalid_content_range_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_resume_with_unexpected_status_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[], headers={}, status_code=204)])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="server returned HTTP 204"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_missing_content_length_on_fresh_download_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[zip_content], headers={})])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Missing Content-Length"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_invalid_content_length_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        zip_content = self._zip(tmp_path)
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[zip_content], headers={"content-length": "many"})])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Invalid Content-Length"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_overlong_body_discards_partial_and_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")
        assert not (tmp_path / "2024-03.Cnaes.zip").exists()

    def test_stale_partial_from_other_month_is_not_resumed(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert "Range" not in scripted_get.calls[0]["headers"]
        assert other_month_part.read_bytes() == b"stale bytes from another month"

    def test_preexisting_final_zip_is_replaced(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.keep_files = False
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        stale_final = tmp_path / "2024-03.Cnaes.zip"
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

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1

    def test_206_without_content_length_is_accepted(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert (tmp_path / "2024-03.Cnaes.zip").read_bytes() == zip_content

    def test_206_missing_content_range_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        (tmp_path / "Cnaes.zip.2024-03.part").write_bytes(zip_content[:10])
        scripted_get = _ScriptedGet([_ScriptedResponse(chunks=[], headers={}, status_code=206)])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadIncompleteError, match="Missing Content-Range"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_206_reversed_content_range_raises(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_416_with_malformed_content_range_discards_partial(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")
        assert not part_path.exists()

    def test_zip_with_corrupt_member_crc_is_rejected(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_corrupt_cached_zip_is_redownloaded(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.keep_files = True
        zip_content = bytearray(_create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test-payload-long-enough"}))
        good_zip = bytes(zip_content)
        marker = zip_content.find(b"0111301")
        zip_content[marker] ^= 0xFF
        (tmp_path / "2024-03.Cnaes.zip").write_bytes(bytes(zip_content))
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[good_zip],
                    headers={"content-length": str(len(good_zip))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert len(scripted_get.calls) == 1
        assert (tmp_path / "2024-03.Cnaes.zip").read_bytes() == good_zip

    def test_cleanup_preserves_part_files(self, downloader: DownloadProbe, tmp_path: Path) -> None:
        downloader.config.keep_files = False
        with zipfile.ZipFile(tmp_path / "2024-03.Cnaes.zip", "w") as z:
            z.writestr("CNAECSV", "01;One\n")
        downloader.config.keep_files = True
        downloader.download_file("2024-03", "Cnaes.zip")
        downloader.config.keep_files = False
        (tmp_path / "Empresas0.zip.2024-03.part").write_bytes(b"resume me")

        downloader.cleanup()

        assert not (tmp_path / "2024-03.Cnaes.zip").exists()
        assert (tmp_path / "Empresas0.zip.2024-03.part").exists()

    def test_empty_keepalive_chunks_past_stall_timeout_raise(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
            downloader.download_and_extract("2024-03", "Cnaes.zip")

    def test_connection_error_read_timeout_maps_to_stall_and_resumes(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert scripted_get.calls[1]["headers"]["Range"] == f"bytes={split_at}-"

    def test_other_connection_errors_propagate_unmapped(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        scripted_get = _ScriptedGet(
            [
                _ScriptedResponse(
                    chunks=[requests.exceptions.ConnectionError("Connection reset by peer")],
                    headers={"content-length": str(len(zip_content))},
                )
            ]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(requests.exceptions.ConnectionError, match="reset by peer"):
            downloader.download_and_extract("2024-03", "Cnaes.zip")


class TestReadTimeoutDetection:
    def test_typed_read_timeout_in_args(self) -> None:
        inner = urllib3.exceptions.ReadTimeoutError(
            urllib3.HTTPConnectionPool("example.com"), "/url", "the socket gave up"
        )
        assert DownloadProbe.is_read_timeout(requests.exceptions.ConnectionError(inner))

    def test_typed_read_timeout_in_cause_chain(self) -> None:
        exc = requests.exceptions.ConnectionError("proxy failure")
        exc.__cause__ = TimeoutError("no matching phrase here")
        assert DownloadProbe.is_read_timeout(exc)

    def test_string_fallback_still_matches(self) -> None:
        assert DownloadProbe.is_read_timeout(requests.exceptions.ConnectionError("Read timed out."))

    def test_unrelated_connection_error_is_not_a_timeout(self) -> None:
        assert not DownloadProbe.is_read_timeout(requests.exceptions.ConnectionError("Connection reset by peer"))

    def test_self_referencing_chain_terminates(self) -> None:
        exc = requests.exceptions.ConnectionError("reset")
        exc.__context__ = exc
        assert not DownloadProbe.is_read_timeout(exc)


class TestStalePartialPruning:
    def test_download_file_prunes_partials_from_other_directories(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stale = downloader.temp_path / "Empresas0.zip.2026-05.part"
        stale.write_bytes(b"stale month")
        same_month_other_file = downloader.temp_path / "Empresas0.zip.2026-06.part"
        same_month_other_file.write_bytes(b"current month")
        unrelated = downloader.temp_path / "not-ours.part"
        unrelated.write_bytes(b"someone else's file")

        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        scripted_get = _ScriptedGet(
            [_ScriptedResponse(chunks=[zip_content], headers={"content-length": str(len(zip_content))})]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        downloader.download_file("2026-06", "Cnaes.zip")

        assert not stale.exists()
        assert same_month_other_file.exists()
        assert unrelated.exists()

    def test_download_files_prunes_partials_from_other_directories(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stale = downloader.temp_path / "Socios9.zip.2026-05.part"
        stale.write_bytes(b"stale month")

        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        scripted_get = _ScriptedGet(
            [_ScriptedResponse(chunks=[zip_content], headers={"content-length": str(len(zip_content))})]
        )
        monkeypatch.setattr(requests, "get", scripted_get)

        results = list(downloader.download_files("2026-06", ["Cnaes.zip"]))

        assert len(results) == 1
        assert not stale.exists()


class TestRetryBudgetResetsOnProgress:
    """Observed live 2026-07-08: Receita stalls big files every few dozen MB.
    Each stall resumes with progress, so a fixed total-failure budget made
    large files structurally undownloadable once stalls > retry_attempts."""

    def test_more_stalls_than_retry_attempts_complete_when_each_makes_progress(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.keep_files = True
        downloader.config.retry_attempts = 2
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test" * 200})
        total = len(zip_content)
        cuts = [0, total // 4, total // 2, (total * 3) // 4]
        responses: list[_ScriptedResponse | Exception] = []
        for i, start in enumerate(cuts):
            end = cuts[i + 1] if i + 1 < len(cuts) else total
            chunks: list[bytes | Exception] = [zip_content[start:end]]
            if end < total:
                chunks.append(requests.exceptions.Timeout("stalled"))
            headers = {"content-length": str(total - start)}
            status = 200
            if start:
                headers["content-range"] = f"bytes {start}-{total - 1}/{total}"
                status = 206
            responses.append(_ScriptedResponse(chunks=chunks, headers=headers, status_code=status))
        scripted_get = _ScriptedGet(responses)
        monkeypatch.setattr(requests, "get", scripted_get)

        result = downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(result) == 1
        assert len(scripted_get.calls) == 4  # 3 stalls survived a budget of 2

    def test_consecutive_no_progress_stalls_still_exhaust_the_budget(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 2
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})

        def stall():
            return _ScriptedResponse(
                chunks=[requests.exceptions.Timeout("wedged")],
                headers={"content-length": str(len(zip_content))},
            )

        scripted_get = _ScriptedGet([stall(), stall()])
        monkeypatch.setattr(requests, "get", scripted_get)

        with pytest.raises(DownloadStalledError):
            downloader.download_and_extract("2024-03", "Cnaes.zip")

        assert len(scripted_get.calls) == 2


class TestAttemptLevelConcurrency:
    """Live failure 2026-07-09: after degradation to 1, four in-flight file
    workers kept reconnecting in parallel every ~35s and Receita answered
    with connect timeouts until every retry budget died. Degradation must
    govern attempts, not just file submissions."""

    def test_stream_permit_blocks_attempts_beyond_degraded_concurrency(self) -> None:
        import threading

        adaptive = AdaptiveDownloadConcurrency(2, 1)
        first_holds = threading.Event()
        release_first = threading.Event()
        second_acquired = threading.Event()

        def first():
            with adaptive.stream_permit():
                first_holds.set()
                release_first.wait(timeout=5)

        def second():
            with adaptive.stream_permit():
                second_acquired.set()

        t1 = threading.Thread(target=first)
        t1.start()
        assert first_holds.wait(timeout=5)

        adaptive.record_stall()
        assert adaptive.current_concurrency == 1

        t2 = threading.Thread(target=second)
        t2.start()
        assert not second_acquired.wait(timeout=0.2), "second attempt ran despite degradation to 1"

        release_first.set()
        assert second_acquired.wait(timeout=5)
        t1.join(timeout=5)
        t2.join(timeout=5)

    def test_connect_timeout_counts_as_adaptive_stall_signal(
        self, downloader: DownloadProbe, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.retry_attempts = 1
        adaptive = AdaptiveDownloadConcurrency(4, 3)
        monkeypatch.setattr(
            requests,
            "get",
            MagicMock(side_effect=requests.exceptions.ConnectTimeout("refused")),
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            downloader.download_zip(
                "https://example/x.zip",
                "2024-03",
                "Cnaes.zip",
                downloader.temp_path / "Cnaes.zip",
                logging.getLogger(__name__).debug,
                adaptive,
            )

        assert adaptive.stall_count == 1

    def test_no_progress_retries_back_off_exponentially_with_a_cap(
        self, downloader: DownloadProbe, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import downloader as downloader_module

        downloader.config.retry_attempts = 4
        downloader.config.retry_delay = 40
        sleeps: list[float] = []
        monkeypatch.setattr(downloader_module.time, "sleep", sleeps.append)
        monkeypatch.setattr(
            requests,
            "get",
            MagicMock(side_effect=requests.exceptions.ConnectTimeout("refused")),
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            downloader.download_zip(
                "https://example/x.zip",
                "2024-03",
                "Cnaes.zip",
                downloader.temp_path / "Cnaes.zip",
                logging.getLogger(__name__).debug,
                None,
            )

        assert sleeps == [40, 80, 120]  # 40*2^2=160 capped at MAX_RETRY_BACKOFF_SECONDS

    def test_stall_is_recorded_while_the_permit_is_still_held(
        self, downloader: DownloadProbe, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Waiters must wake to the degraded limit; recording after release
        races one more attempt through at the old concurrency."""
        downloader.config.retry_attempts = 1
        adaptive = ConcurrencyProbe(4, 1)
        active_at_record: list[int] = []
        original = adaptive.record_stall

        def spy():
            active_at_record.append(adaptive.active_streams)
            return original()

        monkeypatch.setattr(adaptive, "record_stall", spy)
        monkeypatch.setattr(
            requests,
            "get",
            MagicMock(side_effect=requests.exceptions.ConnectTimeout("refused")),
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            downloader.download_zip(
                "https://example/x.zip",
                "2024-03",
                "Cnaes.zip",
                downloader.temp_path / "Cnaes.zip",
                logging.getLogger(__name__).debug,
                adaptive,
            )

        assert active_at_record == [1]


@pytest.mark.parametrize("href", ["", "<d:href/>"])
@pytest.mark.parametrize("list_files", [False, True])
def test_webdav_response_requires_href(downloader: DownloadProbe, href: str, list_files: bool) -> None:
    response = MagicMock()
    response.content = f'<d:multistatus xmlns:d="DAV:"><d:response>{href}</d:response></d:multistatus>'.encode()
    with patch("requests.request", return_value=response):
        with pytest.raises(ValueError, match="WebDAV response is missing href text"):
            if list_files:
                downloader.get_directory_files("2024-11")
            else:
                downloader.get_available_directories()


class TestDownloadSourceIntegrity:
    @pytest.mark.parametrize("members", [{}, {"README.txt": "no source"}, {"CNAECSV/": ""}])
    def test_rejected_zip_is_redownloaded_after_source_is_repaired(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        members: dict[str, str],
    ) -> None:
        downloader.config.keep_files = True
        rejected = _create_test_zip(tmp_path, members)
        repaired = _create_test_zip(tmp_path, {"CNAECSV.csv": "01;Repaired source\n"})
        http = _ScriptedGet(
            [_ScriptedResponse([body], {"Content-Length": str(len(body))}) for body in [rejected, repaired]]
        )
        monkeypatch.setattr(requests, "get", http)

        with pytest.raises(ValueError, match="No recognized source files"):
            downloader.download_file("2024-03", "Cnaes.zip")

        result = downloader.download_file("2024-03", "Cnaes.zip")

        assert result[0].read_text() == "01;Repaired source\n"
        assert len(http.calls) == 2
        assert (tmp_path / "2024-03.Cnaes.zip").read_bytes() == repaired

    def test_kept_zips_are_reused_only_for_the_same_month(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.keep_files = True
        march = _create_test_zip(tmp_path, {"CNAECSV.csv": "01;March\n"})
        april = _create_test_zip(tmp_path, {"CNAECSV.csv": "01;April\n"})
        http = _ScriptedGet([_ScriptedResponse([body], {"Content-Length": str(len(body))}) for body in [march, april]])
        monkeypatch.setattr(requests, "get", http)

        for month, expected in [("2024-03", "March"), ("2024-04", "April"), ("2024-03", "March")]:
            result = downloader.download_file(month, "Cnaes.zip")
            assert result[0].read_text() == f"01;{expected}\n"

        assert [call["url"] for call in http.calls] == [
            f"{downloader.config.base_url}/2024-03/Cnaes.zip",
            f"{downloader.config.base_url}/2024-04/Cnaes.zip",
        ]

    def test_legacy_cache_without_month_is_not_reused(
        self, downloader: DownloadProbe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        downloader.config.keep_files = True
        legacy = _create_test_zip(tmp_path, {"CNAECSV.csv": "01;Unknown month\n"})
        (tmp_path / "Cnaes.zip").write_bytes(legacy)
        current = _create_test_zip(tmp_path, {"CNAECSV.csv": "01;Current month\n"})
        http = _ScriptedGet([_ScriptedResponse([current], {"Content-Length": str(len(current))})])
        monkeypatch.setattr(requests, "get", http)

        result = downloader.download_file("2024-03", "Cnaes.zip")

        assert result[0].read_text() == "01;Current month\n"
        assert len(http.calls) == 1
        assert (tmp_path / "Cnaes.zip").read_bytes() == legacy

    @pytest.mark.parametrize("keep_files", [False, True])
    @pytest.mark.parametrize("entry", ["single", "reference_batch", "parallel_batch"])
    @pytest.mark.parametrize("members", [{}, {"README.txt": "no source"}, {"CNAECSV/": ""}])
    def test_zip_without_source_files_fails(
        self,
        downloader: DownloadProbe,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        keep_files: bool,
        entry: str,
        members: dict[str, str],
    ) -> None:
        downloader.config.keep_files = keep_files
        body = _create_test_zip(tmp_path, members)
        http = _ScriptedGet([_ScriptedResponse([body], {"Content-Length": str(len(body))})])
        monkeypatch.setattr(requests, "get", http)
        filename = "Empresas0.zip" if entry == "parallel_batch" else "Cnaes.zip"
        with pytest.raises(ValueError, match=f"No recognized source files in 2024-03/{filename}"):
            if entry == "single":
                downloader.download_file("2024-03", filename)
            else:
                list(downloader.download_files("2024-03", [filename]))
        assert len(http.calls) == 1
        assert not (tmp_path / f"2024-03.{filename}").exists()


def test_cleanup_preserves_unrelated_files(config: Config, tmp_path: Path) -> None:
    files = {"notes.txt": b"keep notes", "foreign.zip": b"keep archive", "other.csv": b"keep source"}
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    Downloader(config).cleanup()
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == files


def test_cleanup_removes_partially_extracted_owned_file(config: Config, tmp_path: Path) -> None:
    archive = tmp_path / "2024-03.Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("nested/CNAECSV", "01;One\n")
    config.keep_files = True
    downloader = Downloader(config)

    def fail_extract(_zip: zipfile.ZipFile, member: str, path: Path) -> str:
        target = path / member
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("partial")
        raise OSError("disk full")

    with patch.object(zipfile.ZipFile, "extract", fail_extract):
        with pytest.raises(OSError, match="disk full"):
            downloader.download_file("2024-03", "Cnaes.zip")
    config.keep_files = False
    downloader.cleanup()
    assert not archive.exists()
    assert not (tmp_path / "nested/CNAECSV").exists()


@pytest.mark.parametrize("member", ["../CNAECSV", "/CNAECSV", "nested/../CNAECSV"])
def test_rejects_archive_paths_that_change_during_extraction(config: Config, tmp_path: Path, member: str) -> None:
    archive = tmp_path / "2024-03.Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(member, "01;One\n")
    config.keep_files = True
    with pytest.raises(ValueError, match="Unsafe archive member"):
        Downloader(config).download_file("2024-03", "Cnaes.zip")


def test_extraction_does_not_follow_symlink_outside_temp(config: Config, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "CNAECSV"
    sentinel.write_text("keep me")
    workspace = tmp_path / "downloads"
    workspace.mkdir()
    (workspace / "linked").symlink_to(outside, target_is_directory=True)
    archive = workspace / "2024-03.Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("linked/CNAECSV", "overwrite")
    config.temp_dir = str(workspace)
    config.keep_files = True
    downloader = Downloader(config)
    with pytest.raises(ValueError, match="Unsafe archive member"):
        downloader.download_file("2024-03", "Cnaes.zip")
    config.keep_files = False
    downloader.cleanup()
    assert sentinel.read_text() == "keep me"


def test_cleanup_keeps_downloaded_artifacts_when_requested(config: Config, tmp_path: Path) -> None:
    archive = tmp_path / "2024-03.Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("nested/CNAECSV", "01;One\n")
    config.keep_files = True
    downloader = Downloader(config)
    paths = downloader.download_file("2024-03", "Cnaes.zip")
    downloader.cleanup()
    assert archive.exists()
    assert paths[0].read_text() == "01;One\n"


@pytest.mark.parametrize("directory_link", [False, True])
def test_extraction_rejects_internal_symlink_alias(config: Config, tmp_path: Path, directory_link: bool) -> None:
    original = tmp_path / "original"
    original.mkdir()
    sentinel = original / "CNAECSV"
    sentinel.write_text("keep me")
    alias = tmp_path / ("linked" if directory_link else "CNAECSV")
    alias.symlink_to(original if directory_link else sentinel, target_is_directory=directory_link)
    member = "linked/CNAECSV" if directory_link else "CNAECSV"
    archive = tmp_path / "2024-03.Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(member, "overwrite")
    config.keep_files = True
    downloader = Downloader(config)
    with pytest.raises(ValueError, match="Unsafe archive member"):
        downloader.download_file("2024-03", "Cnaes.zip")
    config.keep_files = False
    downloader.cleanup()
    assert sentinel.read_text() == "keep me"
    assert alias.is_symlink()
    assert not archive.exists()


def test_cleanup_preserves_directory_after_extraction_failure(config: Config, tmp_path: Path) -> None:
    target = tmp_path / "CNAECSV"
    target.mkdir()
    sentinel = target / "notes.txt"
    sentinel.write_text("keep me")
    archive = tmp_path / "2024-03.Cnaes.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("CNAECSV", "01;One\n")
    config.keep_files = True
    downloader = Downloader(config)
    with pytest.raises(IsADirectoryError):
        downloader.download_file("2024-03", "Cnaes.zip")
    config.keep_files = False
    downloader.cleanup()
    downloader.cleanup()
    assert target.is_dir()
    assert sentinel.read_text() == "keep me"
    assert not archive.exists()
