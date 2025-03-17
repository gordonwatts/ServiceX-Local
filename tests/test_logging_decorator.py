import logging
from pathlib import Path

from pytest import LogCaptureFixture
from servicex_local.logging_decorator import log_to_file


def test_logfile_written(tmp_path: Path, caplog: LogCaptureFixture):
    log = tmp_path / "func_log.txt"

    @log_to_file(log)
    def run_me():
        logging.info("info")
        logging.debug("debug")
        logging.warning("warning")
        logging.error("error")

    with caplog.at_level(logging.WARNING):
        run_me()

    assert len(caplog.records) == 2
    for r in caplog.records:
        assert r.msg in ["warning", "error"]

    lines = [ln for ln in log.read_text().split("\n") if len(ln.strip()) != 0]
    assert len(lines) == 4


def test_logfile_no_mess_with_levels(tmp_path: Path, caplog: LogCaptureFixture):
    log = tmp_path / "func_log.txt"

    @log_to_file(log)
    def run_me():
        logging.info("info")
        logging.debug("debug")
        logging.warning("warning")
        logging.error("error")

    root_logger = logging.getLogger()
    with caplog.at_level(logging.WARNING):
        assert root_logger.level == logging.WARNING
        run_me()
        assert root_logger.level == logging.WARNING

    assert root_logger.level == logging.WARNING
