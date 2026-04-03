"""Matching service package."""

from app.services.matching_service.matcher import (
    DuplicateMatcher,
    configure_matcher,
    find_best_duplicate,
)

__all__ = ["DuplicateMatcher", "configure_matcher", "find_best_duplicate"]
