from __future__ import annotations

ACTIVE_STATUSES = {
    "acquired",
    "deploying",
    "deployed_for_qa",
    "prod_deploying",
    "stale",
}

TERMINAL_STATUSES = {
    "released",
    "deploy_failed",
    "qa_failed",
    "prod_failed",
    "done",
    "cancelled",
    "stale_released",
    "force_released",
    "superseded",
}

QUEUE_ACTIVE_STATUSES = {
    "queued",
    "deploying",
}

QUEUE_TERMINAL_STATUSES = {
    "deployed",
    "failed",
    "cancelled",
    "superseded",
}

RELEASE_REASON_TO_STATUS = {
    "deploy_failed": "deploy_failed",
    "qa_failed": "qa_failed",
    "prod_failed": "prod_failed",
    "done": "done",
    "cancelled": "cancelled",
    "stale_released": "stale_released",
    "manual_release": "released",
    "superseded": "superseded",
}

RELEASE_REASON_ALLOWED_FROM = {
    "deploy_failed": {"deploying", "stale"},
    "qa_failed": {"deployed_for_qa", "stale"},
    "prod_failed": {"prod_deploying", "stale"},
    "done": {"prod_deploying"},
    "cancelled": ACTIVE_STATUSES,
    "stale_released": {"stale"},
    "manual_release": ACTIVE_STATUSES,
    "superseded": {"deployed_for_qa", "stale"},
}

ALLOWED_TRANSITIONS = {
    "mark_deploying": ({"acquired"}, "deploying"),
    "mark_deployed_for_qa": ({"deploying"}, "deployed_for_qa"),
    "mark_prod_deploying": ({"deployed_for_qa"}, "prod_deploying"),
}

DEFAULT_STALE_AFTER_SECONDS = 2 * 60 * 60
