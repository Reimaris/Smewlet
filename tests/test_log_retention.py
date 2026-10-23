"""Rotated log files beyond the retention count are deleted."""

from pathlib import Path

from app.core.log_files import LOG_BACKUP_COUNT, make_file_handler


def test_old_rotated_logs_are_selected_for_deletion(tmp_path: Path) -> None:
    log = tmp_path / "app.log"
    log.touch()
    days = [f"2026-10-{d:02d}" for d in range(1, 13)]
    for day in days:
        (tmp_path / f"app.log.{day}.log").touch()
    (tmp_path / "other.txt").touch()

    handler = make_file_handler(log)
    doomed = {Path(p).name for p in handler.getFilesToDelete()}
    handler.close()

    expected = {f"app.log.{d}.log" for d in days[: len(days) - LOG_BACKUP_COUNT]}
    assert doomed == expected


def test_rotation_names_files_with_date_and_log_suffix(tmp_path: Path) -> None:
    handler = make_file_handler(tmp_path / "app.log")
    name = handler.rotation_filename(str(tmp_path / "app.log.2026-10-22"))
    handler.close()
    assert handler.suffix == "%Y-%m-%d.log"
    assert name.endswith("2026-10-22")
