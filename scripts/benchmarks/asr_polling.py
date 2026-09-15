"""Offline sensitivity analysis through the real coordinator ASR polling loop.

This is a hypothetical completion-time grid, not replay of provider observations.
No provider clients or network transports are constructed.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from types import SimpleNamespace

from videotrans.podcast.alibaba_asr import AsrPollResult, PollingPolicy
from videotrans.podcast.orchestrator import PodcastCoordinator
from videotrans.podcast.profiles import DEFAULT_PODCAST_PROFILE


def simulate(ready_seconds, policy, request_seconds):
    now = 0.0
    polls = 0
    starts = []

    def sleep(seconds):
        nonlocal now
        now += seconds

    def poll(task_id):
        nonlocal now, polls
        starts.append(now)
        polls += 1
        # Model status sampled at request start, delivered after fixed latency.
        complete = now + 1e-9 >= ready_seconds
        now += request_seconds
        return AsrPollResult(task_id, "SUCCEEDED" if complete else "RUNNING")

    runtime = SimpleNamespace(asr=SimpleNamespace(poll=poll, polling_policy=policy))
    coordinator = PodcastCoordinator(
        runtime, DEFAULT_PODCAST_PROFILE, clock=lambda: now, sleeper=sleep
    )
    # Successful polls do not write state; errors are not part of this scenario.
    result = coordinator._wait_for_asr("offline-task", {}, Path("unused-offline-path"))
    assert result.is_complete
    assert now + 1e-9 >= ready_seconds
    return {"detected_seconds": now, "poll_count": polls, "poll_starts": starts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    current = PollingPolicy()
    fixed = PollingPolicy(initial_seconds=2, maximum_seconds=2, multiplier=1)
    # Known boundaries protect the simulation from accidentally changing cadence.
    assert simulate(9.5, current, 0)["poll_starts"] == [0, 2, 5, 9.5]
    assert simulate(9.51, current, 0)["detected_seconds"] == 14.5
    assert simulate(9.51, fixed, 0)["detected_seconds"] == 10
    assert simulate(4.99, current, 0)["detected_seconds"] == 5
    assert simulate(4.99, fixed, 0)["detected_seconds"] == 6
    rows = []
    for latency in (0, 0.1, 0.5):
        current_runs = [simulate(i / 100, current, latency) for i in range(1, 3001)]
        fixed_runs = [simulate(i / 100, fixed, latency) for i in range(1, 3001)]
        savings = [a["detected_seconds"] - b["detected_seconds"]
                   for a, b in zip(current_runs, fixed_runs)]
        rows.append({
            "request_latency_seconds": latency,
            "mean_savings_seconds": round(statistics.mean(savings), 6),
            "minimum_savings_seconds": round(min(savings), 6),
            "maximum_savings_seconds": round(max(savings), 6),
            "fixed_faster_cases": sum(s > 1e-9 for s in savings),
            "fixed_slower_cases": sum(s < -1e-9 for s in savings),
            "mean_current_poll_count": round(statistics.mean(r["poll_count"] for r in current_runs), 6),
            "mean_fixed_poll_count": round(statistics.mean(r["poll_count"] for r in fixed_runs), 6),
        })
    output = {
        "schema_version": 1, "cloud_calls": 0,
        "grid": {"start_seconds": 0.01, "end_seconds": 30, "step_seconds": 0.01, "count": 3000},
        "status_sampling": "request start; response after fixed latency",
        "scenarios": rows,
        "limitations": [
            "Completion times are a uniform scenario grid, not an observed probability distribution.",
            "Upload, submit, download, parsing, retries and filesystem costs are excluded.",
            "No claim that current provider quotas permit any new policy.",
            "No historical ASR time decomposition or production speedup is inferred.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
