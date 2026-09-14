"""
bot/process_registry.py — Per-user job slots with per-job cancellation.

A user may run up to ``MAX_CONCURRENT_PROCESSES`` (default 2) download/upload
jobs at the same time. Every job owns a :class:`ProcessSlot` that exists from
the moment its link is accepted — including while the "put caption?" prompt is
still on screen — until the job finishes or is cancelled.

Creating the slot early gives three things for free:

* the concurrency guard can *reserve* capacity instead of only counting, so
  posting three links in quick succession can never start three jobs;
* every job owns its own ``asyncio.Event``, so the "🚫 Batal" button cancels
  just that job (``cancel_all()`` keeps the old ``/cancel`` behaviour);
* the slot id doubles as the caption-state key, so two pending prompts for the
  same user no longer overwrite each other.

``is_cancelled(user_id)`` resolves the caller's own slot through a
``ContextVar``, which asyncio copies into every child task. The existing
polling call sites (including workers started with ``asyncio.gather``) therefore
keep working unchanged and never abort a sibling job.

This module depends on nothing but the stdlib and ``app.config`` so its
self-test can be run on its own:

    python -m app.bot.process_registry
"""
from __future__ import annotations

import asyncio
import contextvars
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.config import MAX_CONCURRENT_PROCESSES

# A slot reserved for a caption prompt that never starts is recycled after this
# many seconds, so a leaked reservation can never consume capacity forever.
RESERVATION_TTL = 1800.0

#: Callback-data prefix of the per-job cancel button (``job_cancel_<sid>``).
CANCEL_CALLBACK_PREFIX = "job_cancel_"


@dataclass
class ProcessSlot:
    """One in-flight (or reserved) job belonging to a single user."""

    sid: str
    user_id: int
    kind: str  # "direct" | "torrent" | "terabox" | "mediafire" | "tg_batch"
    task: Optional[asyncio.Task] = None
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    status_msg: Any = None  # progress/status message (carries the 🚫 button)
    prompt_msg: Any = None  # caption-prompt message (reserved stage)
    created_at: float = field(default_factory=time.time)
    cancelled: bool = False
    started: bool = False

    @property
    def is_running(self) -> bool:
        """True while the job's task is still alive."""
        return self.task is not None and not self.task.done()


#: user_id -> list of live slots. This dict *is* the per-user concurrency gate.
active_user_processes: Dict[int, List[ProcessSlot]] = {}

#: sid -> slot, used by the "🚫 Batal" callback and by adopt_process().
_slots_by_sid: Dict[str, ProcessSlot] = {}

#: Slot of the job whose code is currently running (inherited by child tasks).
_current_slot: contextvars.ContextVar[Optional[ProcessSlot]] = contextvars.ContextVar(
    "current_process_slot", default=None
)


# ---------------------------------------------------------------------------
# Capacity
# ---------------------------------------------------------------------------

def _limit() -> int:
    """Effective per-user limit (never less than 1)."""
    return max(1, MAX_CONCURRENT_PROCESSES)


def active_count(user_id: int) -> int:
    """Number of live jobs (running + reserved) for *user_id*."""
    _prune_stale()
    return len(active_user_processes.get(user_id) or [])


def has_free_slot(user_id: int) -> bool:
    """True when *user_id* may reserve another job slot."""
    return active_count(user_id) < _limit()


def slots_for(user_id: int) -> List[ProcessSlot]:
    """Snapshot of the user's slots (safe to iterate while they change)."""
    return list(active_user_processes.get(user_id) or [])


def get_slot(sid: Optional[str]) -> Optional[ProcessSlot]:
    """Look a slot up by its id (the id used in callback data)."""
    if not sid:
        return None
    return _slots_by_sid.get(sid)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def reserve_process(
    user_id: int,
    kind: str,
    status_msg: Any = None,
    prompt_msg: Any = None,
) -> Optional[ProcessSlot]:
    """Reserve a job slot for *user_id*.

    Returns the new slot, or ``None`` when the user already has ``limit`` live
    jobs. The slot is created *before* the job actually starts (e.g. while the
    caption prompt is on screen) so spammed links cannot overshoot.
    """
    _prune_stale()
    slots = active_user_processes.setdefault(user_id, [])
    if len(slots) >= _limit():
        # Undo the setdefault() when we refuse, so the dict stays clean.
        if not slots:
            active_user_processes.pop(user_id, None)
        return None

    slot = ProcessSlot(
        sid=uuid.uuid4().hex[:10],
        user_id=user_id,
        kind=kind,
        status_msg=status_msg,
        prompt_msg=prompt_msg,
    )
    slots.append(slot)
    _slots_by_sid[slot.sid] = slot
    print(f"[Slots] reserved {slot.sid} ({kind}) for user {user_id} "
          f"({len(slots)}/{_limit()})")
    return slot


def adopt_process(sid: Optional[str], task: Optional[asyncio.Task] = None) -> Optional[ProcessSlot]:
    """Bind the running task to a slot reserved earlier and make it current.

    Called at the top of every download/upload pipeline. Returns ``None`` when
    the slot no longer exists (already cancelled and recycled) — the caller
    should then abort quietly.
    """
    slot = _slots_by_sid.get(sid) if sid else None
    if slot is None:
        return None
    slot.task = task or asyncio.current_task()
    slot.started = True
    _current_slot.set(slot)
    return slot


def bind_current_slot(slot: Optional[ProcessSlot]) -> None:
    """Mark *slot* as the job currently executing in this context."""
    _current_slot.set(slot)


def current_slot() -> Optional[ProcessSlot]:
    """The slot of the job running in the current context (if any)."""
    return _current_slot.get()


def release_process(slot: Optional[ProcessSlot]) -> None:
    """Release one slot. Only ever touches *slot* — never its siblings."""
    if slot is None:
        return
    slots = active_user_processes.get(slot.user_id)
    if slots is not None:
        try:
            slots.remove(slot)
        except ValueError:
            pass
        if not slots:
            active_user_processes.pop(slot.user_id, None)
    _slots_by_sid.pop(slot.sid, None)
    if _current_slot.get() is slot:
        _current_slot.set(None)
    print(f"[Slots] released {slot.sid} ({slot.kind}) for user {slot.user_id}")


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------

def cancel_slot(slot: Optional[ProcessSlot]) -> bool:
    """Cancel exactly one job: flip its event, then cancel its task.

    Returns False when the slot is gone or was already cancelled.
    """
    if slot is None or slot.cancelled:
        return False
    slot.cancelled = True
    slot.cancel.set()
    task = slot.task
    if task is not None and not task.done():
        task.cancel()
    print(f"[Slots] cancelled {slot.sid} ({slot.kind}) for user {slot.user_id}")
    return True


def cancel_all(user_id: int) -> int:
    """Cancel every live job of *user_id* (the old ``/cancel`` behaviour)."""
    slots = slots_for(user_id)
    cancelled = sum(1 for slot in slots if cancel_slot(slot))
    # Reserved-but-never-started slots have no task to interrupt, so drop them.
    for slot in slots:
        if not slot.started:
            release_process(slot)
    return cancelled


def request_cancel(user_id: int) -> int:
    """Alias of :func:`cancel_all` — name kept for the existing import sites."""
    return cancel_all(user_id)


def is_cancelled(user_id: int) -> bool:
    """True when the job running in *this* context has been cancelled.

    The slot is found through the ContextVar (asyncio copies it into child
    tasks such as ``asyncio.gather`` workers). It only falls back to the single
    slot of the user when no slot is bound — never to "a sibling job was
    cancelled", because that would abort an innocent job.
    """
    slot = _current_slot.get()
    if slot is not None and slot.user_id == user_id:
        return slot.cancel.is_set()

    slots = slots_for(user_id)
    if len(slots) == 1:
        return slots[0].cancel.is_set()
    return False


def _prune_stale() -> None:
    """Recycle slots never started, or jobs that ended without releasing."""
    if not active_user_processes:
        return
    now = time.time()
    for user_id in list(active_user_processes.keys()):
        for slot in slots_for(user_id):
            if slot.started and slot.task is not None and slot.task.done():
                print(f"[Slots] job {slot.sid} ({slot.kind}) ended without "
                      f"releasing its slot — recycling")
                release_process(slot)
            elif not slot.started and now - slot.created_at > RESERVATION_TTL:
                print(f"[Slots] recycling stale reservation {slot.sid} "
                      f"({slot.kind}) for user {user_id}")
                slot.cancelled = True
                slot.cancel.set()
                release_process(slot)


# ---------------------------------------------------------------------------
# UI helper
# ---------------------------------------------------------------------------

def cancel_keyboard(sid: Optional[str]):
    """Inline keyboard with the per-job "🚫 Batal" button (None when no sid)."""
    if not sid:
        return None
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🚫 Batal", callback_data=f"{CANCEL_CALLBACK_PREFIX}{sid}")]]
    )


# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------
# No test framework is used in this project, so this block (run with
# ``python -m app.bot.process_registry``) exercises the slot bookkeeping that
# the live handlers depend on: the per-user limit, per-job cancellation
# isolation (including from ``asyncio.gather`` child tasks), cancel-all,
# pending-reservation release and stale-reservation recycling.

async def _settle(task: asyncio.Task):
    """Await *task*, reporting a cancellation instead of propagating it."""
    try:
        return await task
    except asyncio.CancelledError:
        return "cancelled"


def _say(text: str) -> None:
    """Print, degrading gracefully when stdout cannot encode the emoji."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"))


async def _test_job(slot: ProcessSlot, user_id: int, seen: dict) -> str:
    """Job body used by the self-test: bind, probe from a child task, poll.

    The ``try`` starts right after binding (as it does in the real handlers), so
    a cancellation landing on *any* await point still releases the slot.
    """
    adopt_process(slot.sid)
    try:
        async def child():
            seen[slot.sid] = is_cancelled(user_id)

        await asyncio.gather(child())
        while True:
            await asyncio.sleep(0.01)
            if is_cancelled(user_id):
                return "cancelled"
    finally:
        release_process(slot)


async def _leaky_job(slot: ProcessSlot, user_id: int) -> None:
    """Job that is cancelled before reaching its own try/finally (slot leak)."""
    adopt_process(slot.sid)
    await asyncio.sleep(60)
    try:
        pass
    finally:
        release_process(slot)


async def _self_test() -> None:
    user_id = 9001
    passed: list[str] = []

    # 1. Per-user capacity
    a = reserve_process(user_id, "direct")
    b = reserve_process(user_id, "torrent")
    assert a is not None and b is not None, "first two reservations must succeed"
    assert active_count(user_id) == 2, active_count(user_id)
    assert not has_free_slot(user_id), "limit must be enforced"
    assert reserve_process(user_id, "terabox") is None, "third reservation refused"
    passed.append(f"per-user limit of {_limit()} enforced")

    # 2. Each job sees only its own slot (ContextVar reaches child tasks)
    seen: dict = {}
    ta = asyncio.create_task(_test_job(a, user_id, seen))
    tb = asyncio.create_task(_test_job(b, user_id, seen))
    for _ in range(200):
        if len(seen) == 2:
            break
        await asyncio.sleep(0.01)
    assert seen == {a.sid: False, b.sid: False}, seen
    assert not ta.done() and not tb.done()
    passed.append("child tasks inherit their own slot (no cross-talk)")

    # 3. Cancelling job A leaves job B untouched
    assert cancel_slot(a) is True
    assert await asyncio.wait_for(_settle(ta), 3) == "cancelled"
    assert b.cancel.is_set() is False, "sibling job must stay alive"
    assert not tb.done(), "sibling task must keep running"
    assert active_count(user_id) == 1, active_count(user_id)
    assert get_slot(a.sid) is None, "cancelled slot must be forgotten"
    passed.append("🚫 Batal cancels exactly one job")

    # 4. cancel_all() keeps the old /cancel behaviour
    assert cancel_all(user_id) == 1
    assert await asyncio.wait_for(_settle(tb), 3) == "cancelled"
    assert active_count(user_id) == 0, active_user_processes
    passed.append("cancel_all() cancels every job of the user")

    # 5. A reserved-but-never-started slot is cancelled and released
    c = reserve_process(user_id, "torrent")
    assert c is not None
    assert cancel_all(user_id) == 1
    assert active_count(user_id) == 0, active_user_processes
    passed.append("pending reservations are released on cancel")

    # 6. Stale reservations are recycled instead of blocking capacity
    d = reserve_process(user_id, "mediafire")
    assert d is not None
    d.created_at -= RESERVATION_TTL + 1
    assert has_free_slot(user_id), "stale reservation must not block capacity"
    assert active_count(user_id) == 0, active_user_processes
    passed.append("stale reservations are recycled")

    # 7. Unbound context still resolves a lone slot (legacy behaviour)
    e = reserve_process(user_id, "direct")
    assert await _probe_unbound(user_id) is False
    cancel_slot(e)
    assert await _probe_unbound(user_id) is True
    release_process(e)
    assert active_count(user_id) == 0, active_user_processes
    passed.append("single-slot fallback keeps legacy is_cancelled() working")

    # 8. Safety net: a job cancelled before its own try/finally is still freed
    f = reserve_process(user_id, "torrent")
    assert f is not None
    tf = asyncio.create_task(_leaky_job(f, user_id))
    await asyncio.sleep(0.02)
    cancel_slot(f)
    assert await asyncio.wait_for(_settle(tf), 3) == "cancelled"
    assert active_count(user_id) == 0, active_user_processes
    passed.append("leaked slots are recycled by the safety net")

    for line in passed:
        _say(f"  ✅ {line}")
    _say("All process_registry self-tests passed.")


async def _probe_unbound(user_id: int) -> bool:
    """Poll is_cancelled() from a task that never adopted a slot."""
    return is_cancelled(user_id)


if __name__ == "__main__":
    asyncio.run(_self_test())
