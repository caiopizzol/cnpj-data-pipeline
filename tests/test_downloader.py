"""Tests for downloader module."""

import zipfile
from unittest.mock import MagicMock, patch

import pytest
import requests

from config import Config
from downloader import CNPJ_FILE_PATTERNS, Downloader


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
        self.calls.append({"url": url, "headers": kwargs.get("headers") or {}})
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


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

    def test_resumes_after_timeout_with_range_header(self, downloader, tmp_path, monkeypatch):
        """A stalled stream should retry from the bytes already saved in .part."""
        downloader.config.keep_files = True
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
        assert scripted_get.calls[0]["headers"] == {}
        assert scripted_get.calls[1]["headers"] == {"Range": f"bytes={split_at}-"}

    def test_resume_appends_to_existing_partial_file(self, downloader, tmp_path, monkeypatch):
        """A leftover .part file is appended to instead of redownloading from byte zero."""
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        split_at = len(zip_content) // 2
        part_path = tmp_path / "Cnaes.zip.part"
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

        assert scripted_get.calls[0]["headers"] == {"Range": f"bytes={split_at}-"}
        assert (tmp_path / "Cnaes.zip").read_bytes() == zip_content

    def test_server_ignoring_range_discards_partial_and_restarts(self, downloader, tmp_path, monkeypatch):
        """A 200 response to a Range request is a clean restart, not an append."""
        downloader.config.keep_files = True
        zip_content = _create_test_zip(tmp_path, {"CNAECSV.D51213": "0111301;Test"})
        part_path = tmp_path / "Cnaes.zip.part"
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

        assert scripted_get.calls[0]["headers"] == {"Range": f"bytes={len(b'stale-part')}-"}
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
        assert (tmp_path / "Cnaes.zip.part").read_bytes() == zip_content[:second_split]

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
        assert not (tmp_path / "Cnaes.zip.part").exists()


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


def _create_test_zip(tmp_path, files: dict) -> bytes:
    """Helper to create a ZIP file with given contents."""
    zip_path = tmp_path / "temp_test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)

    content = zip_path.read_bytes()
    zip_path.unlink()
    return content
