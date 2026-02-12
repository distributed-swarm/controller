# api/v1/reaper.py
"""
Compatibility shim.

The agent reaper is domain lifecycle logic and lives in controller/lifecycle/reaper.py.
This module remains only to avoid breaking older imports during the transition.
"""

from lifecycle.reaper import start_reaper

__all__ = ["start_reaper"]
