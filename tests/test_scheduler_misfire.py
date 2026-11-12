"""Interval jobs must tolerate a briefly blocked event loop instead of skipping ticks."""

import pytest

from app.main import app, scheduler


@pytest.mark.parametrize("job_id", ["transfer_poller_job", "rate_limit_resume_job"])
async def test_interval_jobs_tolerate_late_start(job_id):
    async with app.router.lifespan_context(app):
        job = scheduler.get_job(job_id)
        assert job is not None
        assert job.misfire_grace_time >= 15
        assert job.coalesce is True
    if scheduler.running:
        scheduler.shutdown(wait=False)
