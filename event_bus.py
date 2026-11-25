"""Shift event bus with per-pad staged signals, seals, corrections and acks.

The public entry point is :class:`EventBus`.  It accepts the same envelope
shape used by the JSONL scenarios: ``{"at": int, "kind": str, "record": dict}``.
Only the Python standard library is required.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

__all__ = [
    "EventBus",
    "ShiftEventBus",
    "ShiftBus",
    "Bus",
    "BusError",
    "ValidationError",
    "NotFoundError",
    "ClaimError",
    "create_bus",
    "replay_file",
    "process_events",
    "process_file",
    "ClientSession",
]


class BusError(ValueError):
    """Base class for contract violations."""


class ValidationError(BusError):
    """Raised when an input record does not satisfy the local contract."""


class NotFoundError(BusError):
    """Raised when a referenced shift, pad or notice does not exist."""


class ClaimError(BusError):
    """Raised when a client tries to claim a non-current formal release."""


def _is_int(value: Any) -> bool:
    return type(value) is int


def _require_int(record: Dict[str, Any], key: str, minimum: Optional[int] = None) -> int:
    if key not in record or not _is_int(record[key]):
        raise ValidationError(f"{key} 必须是整数")
    value = record[key]
    if minimum is not None and value < minimum:
        raise ValidationError(f"{key} 不能小于 {minimum}")
    return value


def _require_str(record: Dict[str, Any], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{key} 必须是非空字符串")
    return value


def _trip_key(trip: Any) -> str:
    return f"{type(trip).__name__}:{trip}"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_gaps(gaps: Any, last_seq: int) -> Tuple[set, List[List[int]], List[str]]:
    """Return gap positions, normalized closed intervals and validation errors."""
    positions: set = set()
    ranges: List[List[int]] = []
    errors: List[str] = []
    if gaps is None:
        gaps = []
    if not isinstance(gaps, list):
        return positions, ranges, ["gaps 必须是区间列表"]

    raw_ranges: List[Any]
    # Convenience form: gaps=[2, 4] means the single closed interval [2, 4].
    if len(gaps) == 2 and all(_is_int(x) for x in gaps) and gaps[0] <= gaps[1]:
        raw_ranges = [gaps]
    else:
        raw_ranges = gaps

    for item in raw_ranges:
        start = end = None
        if isinstance(item, (list, tuple)) and len(item) == 2 and _is_int(item[0]) and _is_int(item[1]):
            start, end = int(item[0]), int(item[1])
        elif isinstance(item, dict):
            raw_start = item.get("start", item.get("from"))
            raw_end = item.get("end", item.get("to", raw_start))
            if _is_int(raw_start) and _is_int(raw_end):
                start, end = int(raw_start), int(raw_end)
        elif isinstance(item, str):
            text = item.strip().replace("..", "-").replace(":", "-").replace("~", "-")
            parts = [part.strip() for part in text.split("-") if part.strip()]
            if len(parts) == 1 and parts[0].isdigit():
                start = end = int(parts[0])
            elif len(parts) == 2 and all(part.isdigit() for part in parts):
                start, end = int(parts[0]), int(parts[1])
        elif _is_int(item):
            start = end = int(item)

        if start is None or end is None or start < 1 or start > end:
            errors.append(f"非法 gap 区间: {item!r}")
            continue
        if end > last_seq or start > last_seq:
            errors.append(f"gap 区间超出 last_seq: [{start},{end}]")
        ranges.append([start, end])
        positions.update(range(start, end + 1))
    return positions, ranges, errors


class EventBus:
    """In-memory event bus and query model.

    The class is intentionally permissive about method names because callers may
    either replay envelopes or use the small domain-specific helpers.
    """

    def __init__(self) -> None:
        self.order = 0
        self.last_at = 0
        self.events: List[Dict[str, Any]] = []
        self.results: List[Dict[str, Any]] = []

        self.shifts: Dict[str, Dict[str, Any]] = {}
        self.shift_versions: Dict[str, List[Dict[str, Any]]] = {}
        self.closed: Dict[str, Dict[str, Any]] = {}

        self.chains: List[Dict[str, Any]] = []
        self.trips: Dict[str, Dict[str, Any]] = {}

        # seals[(shift, pad/lane, source)] = declaration
        self.seals: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self.seal_history: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}

        self.signals: Dict[str, List[Dict[str, Any]]] = {}
        self.current_signal: Dict[str, Dict[str, Any]] = {}
        self.feed: Dict[str, List[Dict[str, Any]]] = {}
        self.reviews: List[Dict[str, Any]] = []
        self.corrections: List[Dict[str, Any]] = []
        self.claims: List[Dict[str, Any]] = []
        self.issues: List[Dict[str, Any]] = []

        self.clients: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.subscribers: List[Tuple[Optional[str], Any]] = []
        self._ids = {"signal": 0, "correction": 0, "review": 0, "claim": 0, "issue": 0}

    # ------------------------------------------------------------------ ids
    def _new_id(self, kind: str, prefix: str) -> str:
        self._ids[kind] += 1
        return f"{prefix}-{self._ids[kind]}"

    def _key(self, shift: Optional[str], pad: Optional[str]) -> str:
        return f"{shift}::{pad}"

    def _issue(
        self,
        code: str,
        message: str,
        *,
        at: Optional[int] = None,
        order: Optional[int] = None,
        blocking: bool = True,
        **refs: Any,
    ) -> Dict[str, Any]:
        issue = {
            "id": self._new_id("issue", "issue"),
            "code": code,
            "message": message,
            "blocking": blocking,
            "status": "open",
            "at": at if at is not None else self.last_at,
            "order": order if order is not None else self.order,
            **refs,
        }
        self.issues.append(issue)
        return issue

    @staticmethod
    def _transient_issue(
        code: str,
        message: str,
        *,
        blocking: bool = True,
        **refs: Any,
    ) -> Dict[str, Any]:
        """Create a derived issue without appending it on every re-evaluation."""
        return {
            "id": None,
            "code": code,
            "message": message,
            "blocking": blocking,
            "status": "open",
            **refs,
        }

    # ------------------------------------------------------------- envelope
    def handle(self, event: Any = None, record: Any = None, at: Optional[int] = None, **kwargs: Any) -> Dict[str, Any]:
        """Apply one ``{at, kind, record}`` envelope and return its result.

        Also accepts ``handle(kind, record, at=...)`` for concise direct calls.
        """
        if isinstance(event, str):
            if record is None:
                record = {}
            if not isinstance(record, dict):
                raise ValidationError("record 必须是对象")
            envelope = {"at": self.last_at if at is None else at, "kind": event, "record": record}
            return self.handle(envelope)
        if event is None and kwargs.get("kind"):
            envelope = {
                "at": kwargs.get("at", self.last_at),
                "kind": kwargs["kind"],
                "record": kwargs.get("record", {}),
            }
            return self.handle(envelope)
        if not isinstance(event, dict):
            raise ValidationError("事件必须是对象")

        # Also accept flattened calls such as handle(kind=..., record=...).
        kind = event.get("kind")
        record = event.get("record", {})
        at = event.get("at", self.last_at)
        if not isinstance(kind, str) or not kind:
            raise ValidationError("kind 必须是非空字符串")
        if not isinstance(record, dict):
            raise ValidationError("record 必须是对象")
        if _is_int(at):
            if at < self.last_at:
                # The file reader rejects this; direct callers get the same rule.
                raise ValidationError("at 必须非递减")
            self.last_at = at
        else:
            at = self.last_at

        self.order += 1
        order = self.order
        handler = {
            "shift": self._cmd_shift,
            "load": self._cmd_load,
            "weigh": self._cmd_weigh,
            "seal": self._cmd_seal,
            "close": self._cmd_close,
            "pull": self._cmd_pull,
            "ack": self._cmd_ack,
            "claim": self._cmd_claim,
            "inspect": self._cmd_inspect,
        }.get(kind)
        if handler is None:
            raise ValidationError(f"未登记的消息名称: {kind}")

        result = handler(record, at, order)
        result.setdefault("ok", True)
        result["kind"] = kind
        result["at"] = at
        result["order"] = order
        self.events.append({"at": at, "kind": kind, "record": record, "order": order})
        self.results.append(result)
        self._emit(kind, result)
        return result

    # Friendly aliases for callers that do not use JSONL envelopes.
    process = handle
    handle_event = handle
    ingest = handle
    publish = handle
    send = handle
    apply_event = handle

    def __call__(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return self.handle(event)

    def subscribe(self, event_type: Any = None, handler: Any = None) -> Any:
        """Subscribe to lifecycle events.

        ``subscribe(callback)`` receives every handled envelope result;
        ``subscribe("formal", callback)`` filters by emitted lifecycle type.
        """
        if handler is None and callable(event_type):
            event_type, handler = None, event_type
        if not callable(handler):
            raise ValidationError("subscribe 需要可调用 handler")
        self.subscribers.append((event_type, handler))
        return handler

    def unsubscribe(self, handler: Any) -> bool:
        before = len(self.subscribers)
        self.subscribers = [(kind, cb) for kind, cb in self.subscribers if cb != handler]
        return len(self.subscribers) < before

    def _emit(self, event_type: str, payload: Dict[str, Any]) -> None:
        for expected_type, handler in list(self.subscribers):
            if expected_type is None or expected_type == event_type or expected_type == "*":
                handler(payload)

    def submit(self, kind: str, record: Dict[str, Any], at: Optional[int] = None) -> Dict[str, Any]:
        return self.handle({"at": self.last_at if at is None else at, "kind": kind, "record": record})

    # --------------------------------------------------------------- shifts
    def _cmd_shift(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        shift_id = _require_str(rec, "shift")
        starts = _require_int(rec, "starts", 0)
        ends = _require_int(rec, "ends", 0)
        if ends <= starts:
            raise ValidationError("ends 必须晚于 starts")
        pads = rec.get("pads")
        if not isinstance(pads, dict) or not pads:
            raise ValidationError("pads 必须是非空对象")
        norm_pads: Dict[str, List[str]] = {}
        for pad, sources in pads.items():
            if not isinstance(pad, str) or not pad:
                raise ValidationError("卸点名称必须是非空字符串")
            if not isinstance(sources, list) or not all(isinstance(s, str) and s for s in sources):
                raise ValidationError(f"pads.{pad} 必须是来源名称数组")
            norm_pads[pad] = list(sources)
        required_weight = _require_int(rec, "required_weight", 0)

        config = {
            "shift": shift_id,
            "starts": starts,
            "ends": ends,
            "pads": norm_pads,
            "required_weight": required_weight,
            "at": at,
            "order": order,
            "version": len(self.shift_versions.get(shift_id, [])) + 1,
        }
        self.shifts[shift_id] = config
        self.shift_versions.setdefault(shift_id, []).append(dict(config))

        # A late shift definition may make earlier pending facts interpretable.
        self._rebuild_trips()
        for pad in norm_pads:
            key = self._key(shift_id, pad)
            self.signals.setdefault(key, [])
            self.feed.setdefault(key, [])
            if shift_id in self.closed:
                self._refresh_key(shift_id, pad, at, order, reason="shift-update")
        return {"shift": shift_id, "version": config["version"], "pads": sorted(norm_pads)}

    add_shift = lambda self, rec, at=None: self.submit("shift", rec, at)  # noqa: E731
    upsert_shift = add_shift

    def _locate_shift(self, observed_at: int) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        versions = [version for history in self.shift_versions.values() for version in history]
        candidates = [s for s in versions if s["starts"] <= observed_at < s["ends"]]
        if not candidates:
            return None, None

        # Prefer the schedule generation that had already been published at the
        # business observation time.  Late supplements therefore keep being
        # interpreted under the historical duty arrangement.
        timely = [s for s in candidates if s["at"] <= observed_at]
        pool = timely or candidates
        chosen = max(pool, key=lambda s: (s["at"], s["version"], s["order"]))
        same_time = [
            s
            for s in pool
            if s["at"] == chosen["at"] and s["version"] == chosen["version"] and s["order"] == chosen["order"]
        ]
        if len({s["shift"] for s in same_time}) > 1:
            return None, None
        return chosen["shift"], chosen

    # -------------------------------------------------------- source facts
    def _cmd_load(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        return self._ingest_source("load", rec, at, order)

    def _cmd_weigh(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        return self._ingest_source("weigh", rec, at, order)

    submit_load = lambda self, rec, at=None: self.submit("load", rec, at)  # noqa: E731
    add_load = submit_load
    record_load = submit_load
    submit_weigh = lambda self, rec, at=None: self.submit("weigh", rec, at)  # noqa: E731
    add_weigh = submit_weigh
    record_weigh = submit_weigh

    def add_message(self, kind: str, record: Dict[str, Any], at: Optional[int] = None) -> Dict[str, Any]:
        return self.submit(kind, record, at)

    record_message = add_message

    def replay_events(self, events: Iterable[Dict[str, Any]]) -> "EventBus":
        """Apply an iterable of envelopes in order and return the bus."""
        for event in events:
            self.handle(event)
        return self

    process_events = replay_events
    load_events = replay_events

    def post(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return self.handle(event)

    post_event = handle

    def _find_chain(
        self, source: str, lane: str, seq: int, rev: int, derived_shift: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        matches = [c for c in self.chains if c["source"] == source and c["lane"] == lane and c["seq"] == seq]
        if not matches:
            return None
        if derived_shift is not None:
            exact = [c for c in matches if c.get("shift_id") == derived_shift]
            if len(exact) == 1:
                return exact[0]
            if len(exact) > 1:
                return max(exact, key=lambda c: c["uid"])
            # A higher revision can correct a trip into another shift.  A rev=1
            # message with the same seq in a different shift is a new numbered
            # channel, not a same-rev conflict on the old shift.
            upgradable = [c for c in matches if c["highest_rev"] < rev]
            if upgradable:
                return max(upgradable, key=lambda c: (c["highest_rev"], c["uid"]))
            return None

        # Before a weigh record's trip/start is known, use revision order to
        # locate its chain; rebuild_trips() assigns the shift later.
        upgradable = [c for c in matches if c["highest_rev"] < rev]
        if upgradable:
            return max(upgradable, key=lambda c: (c["highest_rev"], c["uid"]))
        with_rev = [c for c in matches if any(v["rev"] == rev for v in c["versions"])]
        if with_rev:
            return max(with_rev, key=lambda c: c["uid"])
        return max(matches, key=lambda c: (c["highest_rev"], c["uid"]))

    def _ingest_source(self, kind: str, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        source = _require_str(rec, "source")
        seq = _require_int(rec, "seq", 1)
        rev = _require_int(rec, "rev", 1)
        trip = rec.get("trip")
        if trip is None or (not isinstance(trip, (str, int))):
            raise ValidationError("trip 必须是字符串或整数运输号")
        lane = rec.get("lane")
        if lane is None and kind == "load":
            lane = rec.get("pad")
        if not isinstance(lane, str) or not lane:
            raise ValidationError("lane 必须是非空原始卸点通道")

        observed_at: Optional[int] = None
        pad: Optional[str] = None
        kg: Optional[int] = None
        if kind == "load":
            observed_at = _require_int(rec, "observed_at", 0)
            pad = _require_str(rec, "pad")
            derived_shift, _ = self._locate_shift(observed_at)
        else:
            kg = _require_int(rec, "kg", 0)
            tv = self.trips.get(_trip_key(trip))
            derived_shift = tv.get("shift_id") if tv else rec.get("shift")
            if derived_shift is not None and derived_shift not in self.shifts:
                derived_shift = None

        chain = self._find_chain(source, lane, seq, rev, derived_shift)
        is_new = chain is None
        if is_new:
            chain = {
                "uid": len(self.chains) + 1,
                "kind": kind,
                "source": source,
                "lane": lane,
                "seq": seq,
                "shift_id": derived_shift,
                "first_observed_at": observed_at,
                "first_order": order,
                "highest_rev": 0,
                "versions": [],
                "attempts": [],
                "open_conflict": None,
                "conflicts": [],
            }
            self.chains.append(chain)
        elif chain["kind"] != kind:
            raise ValidationError(f"{source}/{lane}/{seq} 的消息类型不能从 {chain['kind']} 变为 {kind}")

        before_keys = self._affected_keys_for_chain(chain)
        previous = dict(chain["current"]) if chain.get("current") else None
        raw = json.loads(json.dumps(rec, ensure_ascii=False))
        attempt = {"at": at, "order": order, "rev": rev, "record": raw}
        chain["attempts"].append(attempt)

        result: Dict[str, Any] = {
            "source": source,
            "lane": lane,
            "seq": seq,
            "rev": rev,
            "trip": trip,
            "new": is_new,
        }

        same_rev_current = next((v for v in chain["versions"] if v["rev"] == rev), None)
        if rev < chain["highest_rev"]:
            result.update(status="stale_revision", accepted=False)
            attempt["classification"] = "stale"
            # A contradictory late copy of an old revision is audited but cannot block current work.
            if same_rev_current is not None and _canonical(same_rev_current["raw"]) != _canonical(raw):
                issue = self._issue(
                    "stale_revision_conflict",
                    "晚到的旧修订内容不一致，但不能覆盖更新版本",
                    at=at,
                    order=order,
                    blocking=False,
                    source=source,
                    lane=lane,
                    seq=seq,
                    rev=rev,
                )
                chain["conflicts"].append(issue)
                result["issue_id"] = issue["id"]
            return result

        if rev == chain["highest_rev"] and chain.get("current") is not None:
            current_raw = chain["current"]["raw"]
            if _canonical(current_raw) == _canonical(raw):
                result.update(status="duplicate", accepted=False)
                attempt["classification"] = "duplicate"
                return result

            # If this exact competing content was already reported, stay idempotent.
            if chain.get("open_conflict") and _canonical(chain["open_conflict"].get("competing_raw")) == _canonical(raw):
                result.update(status="conflict_duplicate", accepted=False, issue_id=chain["open_conflict"]["id"])
                return result

            issue = self._issue(
                "revision_conflict",
                "同一 rev 出现不同内容，已保留冲突，等待更高修订",
                at=at,
                order=order,
                blocking=True,
                source=source,
                lane=lane,
                seq=seq,
                rev=rev,
                existing_order=chain["current"]["order"],
                competing_order=order,
                competing_raw=raw,
            )
            chain["open_conflict"] = issue
            chain["conflicts"].append(issue)
            result.update(status="conflict", accepted=False, issue_id=issue["id"])
            self._rebuild_trips()
            for sid, pad_name in sorted(before_keys | self._affected_keys_for_chain(chain)):
                self._refresh_key(sid, pad_name, at, order, reason="conflict")
            return result

        # rev > highest_rev, or first accepted revision.
        resolved_issue = chain.get("open_conflict")
        if resolved_issue is not None:
            resolved_issue["status"] = "resolved"
            resolved_issue["resolved_at"] = at
            resolved_issue["resolved_order"] = order
            resolved_issue["resolved_by_rev"] = rev
        for issue in chain.get("conflicts", []):
            if issue.get("status") == "open" and issue.get("code") == "revision_conflict":
                issue["status"] = "resolved"
                issue["resolved_at"] = at
                issue["resolved_order"] = order
                issue["resolved_by_rev"] = rev
        chain["open_conflict"] = None

        if kind == "load" and chain.get("first_observed_at") is None:
            chain["first_observed_at"] = observed_at
        accepted_observed = chain.get("first_observed_at") if kind == "load" else None
        if kind == "load" and observed_at != chain.get("first_observed_at"):
            warning = self._issue(
                "observed_time_immutable",
                "修订试图改变首次观测时间；仍按首次 observed_at 归班",
                at=at,
                order=order,
                blocking=False,
                source=source,
                lane=lane,
                seq=seq,
                rev=rev,
                attempted_observed_at=observed_at,
                observed_at=chain.get("first_observed_at"),
            )
            chain["conflicts"].append(warning)
            result["warning_id"] = warning["id"]

        version = {
            "rev": rev,
            "at": at,
            "order": order,
            "raw": raw,
            "trip": trip,
            "pad": pad,
            "kg": kg,
            "observed_at": accepted_observed if kind == "load" else None,
        }
        chain["versions"].append(version)
        chain["current"] = version
        chain["highest_rev"] = rev
        chain["status"] = "current"
        attempt["classification"] = "accepted"
        result["accepted"] = True
        result["status"] = "accepted"

        correction: Optional[Dict[str, Any]] = None
        if previous is not None and rev > previous["rev"]:
            changes: Dict[str, Dict[str, Any]] = {}
            for field_name, old_value, new_value in (
                ("trip", previous.get("trip"), trip),
                ("pad", previous.get("pad"), pad),
                ("kg", previous.get("kg"), kg),
                ("observed_at", previous.get("observed_at"), version["observed_at"]),
            ):
                if old_value != new_value and not (field_name == "observed_at" and kind != "load"):
                    changes[field_name] = {"from": old_value, "to": new_value}
            correction = {
                "id": self._new_id("correction", "corr"),
                "at": at,
                "order": order,
                "kind": kind,
                "source": source,
                "lane": lane,
                "seq": seq,
                "rev": rev,
                "previous_rev": previous["rev"],
                "trip": trip,
                "changes": changes,
                "original_start_fact": self._chain_start_fact(chain),
                "affected_keys": [],
            }
            self.corrections.append(correction)
            result["correction_id"] = correction["id"]

        self._rebuild_trips()
        after_keys = self._affected_keys_for_chain(chain)
        affected = before_keys | after_keys
        if correction is not None:
            correction["affected_keys"] = [{"shift": s, "pad": p} for s, p in sorted(affected) if s is not None]
        for sid, pad_name in sorted(affected):
            if sid is not None and pad_name is not None:
                self._refresh_key(sid, pad_name, at, order, reason="correction" if correction else "source", correction=correction)
        return result

    def _chain_start_fact(self, chain: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if chain["kind"] != "load" or not chain["versions"]:
            return None
        first = chain["versions"][0]
        return {
            "source": chain["source"],
            "seq": chain["seq"],
            "rev": first["rev"],
            "trip": first.get("trip"),
            "observed_at": chain.get("first_observed_at"),
            "pad": first.get("pad"),
            "lane": chain["lane"],
            "at": first.get("at"),
            "order": first.get("order"),
        }

    def _affected_keys_for_chain(self, chain: Dict[str, Any]) -> set:
        keys = set()
        sid = chain.get("shift_id")
        if sid is not None:
            keys.add((sid, chain["lane"]))
        current = chain.get("current")
        if current:
            tv = self.trips.get(_trip_key(current["trip"]))
            if tv:
                if tv.get("shift_id"):
                    keys.add((tv["shift_id"], tv.get("pad") or chain["lane"]))
            elif sid is not None:
                keys.add((sid, chain["lane"]))
        return {(s, p) for s, p in keys if s is not None and p is not None}

    def _rebuild_trips(self) -> None:
        """Rebuild current trip projections from current source revisions."""
        # Loads first so observed time can assign shifts to weigh chains.
        for chain in self.chains:
            current = chain.get("current")
            if chain["kind"] == "load" and current:
                observed = chain.get("first_observed_at")
                if _is_int(observed):
                    sid, _ = self._locate_shift(observed)
                    chain["shift_id"] = sid

        grouped: Dict[str, Dict[str, Any]] = {}
        for chain in self.chains:
            current = chain.get("current")
            if not current:
                continue
            key = _trip_key(current["trip"])
            bucket = grouped.setdefault(
                key,
                {"trip": current["trip"], "key": key, "load_chains": [], "weigh_chains": [], "issues": []},
            )
            if chain["kind"] == "load":
                bucket["load_chains"].append(chain)
            else:
                bucket["weigh_chains"].append(chain)

        next_trips: Dict[str, Dict[str, Any]] = {}
        for key, bucket in grouped.items():
            loads = bucket["load_chains"]
            weighs = bucket["weigh_chains"]
            start_fact = None
            shift_id = None
            observed_at = None
            pad = None
            issues: List[Dict[str, Any]] = []

            if loads:
                first_chain = min(loads, key=lambda c: (c.get("first_order", 10**18), c["uid"]))
                observed_at = first_chain.get("first_observed_at")
                start_fact = self._chain_start_fact(first_chain)
                times = {c.get("first_observed_at") for c in loads}
                if len(times) > 1:
                    issue = self._transient_issue(
                        "start_time_conflict",
                        "同一运输的首次观测时间不一致",
                        blocking=True,
                        trip=bucket["trip"],
                        observed_values=sorted(v for v in times if _is_int(v)),
                    )
                    issues.append(issue)
                if _is_int(observed_at):
                    shift_id, shift = self._locate_shift(observed_at)
                    if shift is None:
                        issue = self._transient_issue(
                            "shift_not_found",
                            "观测时间不属于任何已登记班次",
                            blocking=True,
                            trip=bucket["trip"],
                            observed_at=observed_at,
                        )
                        issues.append(issue)
                pads = {c["current"].get("pad") for c in loads if c.get("current")}
                if len(pads) > 1:
                    issue = self._transient_issue(
                        "pad_association_conflict",
                        "同一运输的当前 load 指向不同卸点",
                        blocking=True,
                        trip=bucket["trip"],
                        pads=sorted(p for p in pads if p),
                    )
                    issues.append(issue)
                    latest_chain = max(loads, key=lambda c: (c["current"]["order"], c["uid"]))
                    pad = latest_chain["current"].get("pad")
                elif pads:
                    pad = next(iter(pads))
            else:
                issue = self._transient_issue(
                    "missing_start_fact",
                    "运输已有证据但缺少带 observed_at 的 load 开始事实",
                    blocking=True,
                    trip=bucket["trip"],
                )
                issues.append(issue)

            kg = 0
            weigh_refs = []
            for chain in weighs:
                cur = chain["current"]
                kg += int(cur.get("kg") or 0)
                weigh_refs.append(self._ref(chain, cur))
                if chain.get("open_conflict"):
                    issues.append(chain["open_conflict"])
            load_refs = [self._ref(c, c["current"]) for c in loads if c.get("current")]

            tv = {
                "key": key,
                "trip": bucket["trip"],
                "shift": shift_id,
                "shift_id": shift_id,
                "observed_at": observed_at,
                "pad": pad,
                "confirmed_kg": kg,
                "load_refs": load_refs,
                "weigh_refs": weigh_refs,
                "source_refs": load_refs + weigh_refs,
                "start_fact": start_fact,
                "issues": [self._issue_brief(i) for i in issues],
                "has_blocking_issue": any(i.get("blocking", True) and i.get("status", "open") == "open" for i in issues),
            }
            next_trips[key] = tv
            for chain in weighs:
                chain["shift_id"] = shift_id
        self.trips = next_trips

    def _ref(self, chain: Dict[str, Any], version: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "kind": chain["kind"],
            "source": chain["source"],
            "lane": chain["lane"],
            "seq": chain["seq"],
            "rev": version["rev"],
            "at": version["at"],
            "order": version["order"],
            "trip": version.get("trip"),
            "pad": version.get("pad"),
            "kg": version.get("kg"),
        }

    @staticmethod
    def _issue_brief(issue: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": issue.get("id"),
            "code": issue.get("code"),
            "message": issue.get("message"),
            "blocking": issue.get("blocking", True),
            "status": issue.get("status", "open"),
        }

    # --------------------------------------------------------------- seals
    def _cmd_seal(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        shift_id = _require_str(rec, "shift")
        pad = _require_str(rec, "pad")
        source = _require_str(rec, "source")
        last_seq = _require_int(rec, "last_seq", 0)
        gap_positions, gap_ranges, errors = parse_gaps(rec.get("gaps", []), last_seq)
        key = (shift_id, pad, source)
        old = self.seals.get(key)

        if errors:
            for message in errors:
                self._issue("invalid_seal", message, at=at, order=order, shift=shift_id, pad=pad, source=source)

        declaration = {
            "shift": shift_id,
            "pad": pad,
            "source": source,
            "last_seq": last_seq,
            "gaps": gap_ranges,
            "gap_positions": sorted(gap_positions),
            "at": at,
            "order": order,
            "valid": not errors,
        }

        if old is not None and last_seq < old["last_seq"]:
            self._issue(
                "stale_seal",
                "晚到的封口位置低于已声明位置，不能缩小来源范围",
                at=at,
                order=order,
                blocking=False,
                shift=shift_id,
                pad=pad,
                source=source,
                attempted_last_seq=last_seq,
                last_seq=old["last_seq"],
            )
            return {"status": "stale_seal", "seal": old}

        self.seal_history.setdefault(key, []).append(dict(old) if old else None)
        declaration["version"] = len([x for x in self.seal_history[key] if x is not None]) + 1
        self.seals[key] = declaration

        self.signals.setdefault(self._key(shift_id, pad), [])
        self.feed.setdefault(self._key(shift_id, pad), [])
        if shift_id in self.shifts and shift_id in self.closed:
            self._refresh_key(shift_id, pad, at, order, reason="seal")
        return {"status": "accepted", "seal": {k: v for k, v in declaration.items() if k != "gap_positions"}}

    seal_source = lambda self, rec, at=None: self.submit("seal", rec, at)  # noqa: E731
    declare_seal = seal_source
    seal_pad = seal_source
    add_seal = seal_source

    # ------------------------------------------------------------ evidence
    def _chains_for_channel(self, shift_id: str, source: str, lane: str) -> List[Dict[str, Any]]:
        return [
            c
            for c in self.chains
            if c.get("shift_id") == shift_id and c["source"] == source and c["lane"] == lane and c.get("current")
        ]

    def evidence_for_source(self, shift_id: str, pad: str, source: str) -> Dict[str, Any]:
        seal = self.seals.get((shift_id, pad, source))
        chains = self._chains_for_channel(shift_id, source, pad)
        by_seq: Dict[int, List[Dict[str, Any]]] = {}
        for chain in chains:
            by_seq.setdefault(chain["seq"], []).append(chain)

        result: Dict[str, Any] = {
            "shift": shift_id,
            "pad": pad,
            "source": source,
            "lane": pad,
            "sealed": seal is not None,
            "complete": False,
            "last_seq": seal["last_seq"] if seal else None,
            "received_seqs": sorted(by_seq),
            "missing_seqs": [],
            "never_produced_seqs": sorted(seal["gap_positions"]) if seal else [],
            "gap_seqs": sorted(seal["gap_positions"]) if seal else [],
            "conflicts": [],
            "source_positions": [],
        }
        if seal is None:
            result["reason"] = "no_seal"
            return result

        if not seal.get("valid", True):
            result["conflicts"].append({"code": "invalid_seal", "status": "open"})

        gap_set = set(seal["gap_positions"])
        for seq in range(1, seal["last_seq"] + 1):
            seq_chains = by_seq.get(seq, [])
            if seq in gap_set:
                if seq_chains:
                    issue = self._transient_issue(
                        "declared_gap_has_message",
                        "封口声明为 gap 的位置实际存在来源消息",
                        blocking=True,
                        shift=shift_id,
                        pad=pad,
                        source=source,
                        lane=pad,
                        seq=seq,
                    )
                    result["conflicts"].append(self._issue_brief(issue))
                else:
                    result["source_positions"].append({"seq": seq, "status": "never_produced"})
                continue

            if not seq_chains:
                result["missing_seqs"].append(seq)
                result["source_positions"].append({"seq": seq, "status": "missing"})
                continue

            if len(seq_chains) > 1:
                issue = self._transient_issue(
                    "duplicate_source_seq",
                    "同一班次/来源/通道/序号出现多条记录",
                    blocking=True,
                    shift=shift_id,
                    pad=pad,
                    source=source,
                    lane=pad,
                    seq=seq,
                )
                result["conflicts"].append(self._issue_brief(issue))

            chain = seq_chains[0]
            status = "received"
            if chain.get("open_conflict"):
                result["conflicts"].append(self._issue_brief(chain["open_conflict"]))
                status = "conflict"
            result["source_positions"].append(
                {"seq": seq, "status": status, "rev": chain["highest_rev"], "uid": chain["uid"]}
            )

        for seq, seq_chains in sorted(by_seq.items()):
            if seq > seal["last_seq"]:
                issue = self._transient_issue(
                    "message_beyond_sealed_range",
                    "来源消息序号超过封口 last_seq，需要更新封口声明",
                    blocking=True,
                    shift=shift_id,
                    pad=pad,
                    source=source,
                    lane=pad,
                    seq=seq,
                    last_seq=seal["last_seq"],
                )
                result["conflicts"].append(self._issue_brief(issue))

        result["declared_empty"] = bool(seal is not None and seal["last_seq"] == 0)
        result["no_record"] = result["declared_empty"] and not by_seq
        result["complete"] = not result["missing_seqs"] and not any(
            c.get("status", "open") == "open" and c.get("blocking", True) for c in result["conflicts"]
        )
        return result

    def evidence_for_pad(self, shift_id: str, pad: str) -> Dict[str, Any]:
        shift = self.shifts.get(shift_id)
        if shift is None:
            raise NotFoundError(f"未知班次: {shift_id}")
        sources = shift["pads"].get(pad, [])
        source_evidence = [self.evidence_for_source(shift_id, pad, source) for source in sources]
        missing_sources = [
            ev["source"]
            for ev in source_evidence
            if not ev["sealed"] or ev["missing_seqs"] or not ev["complete"]
        ]
        issues: List[Dict[str, Any]] = []
        for ev in source_evidence:
            issues.extend(ev.get("conflicts", []))
        return {
            "shift": shift_id,
            "pad": pad,
            "required_sources": list(sources),
            "sources": source_evidence,
            "missing_sources": missing_sources,
            "complete": all(ev["complete"] for ev in source_evidence),
            "issues": issues,
        }

    def missing_evidence(self, shift_id: Optional[str] = None, pad: Optional[str] = None) -> Dict[str, Any]:
        if shift_id is not None:
            if pad is not None:
                return self.evidence_for_pad(shift_id, pad)
            return {
                "shift": shift_id,
                "pads": {name: self.evidence_for_pad(shift_id, name) for name in self.shifts[shift_id]["pads"]},
            }
        return {
            sid: {name: self.evidence_for_pad(sid, name) for name in shift["pads"]}
            for sid, shift in self.shifts.items()
            if pad is None or pad in shift["pads"]
        }

    # ----------------------------------------------------------- evaluation
    def evaluate(self, shift_id: str, pad: str) -> Dict[str, Any]:
        if shift_id not in self.shifts:
            raise NotFoundError(f"未知班次: {shift_id}")
        shift = self.shifts[shift_id]
        evidence = self.evidence_for_pad(shift_id, pad)

        trip_views = []
        for tv in self.trips.values():
            if tv.get("shift_id") == shift_id and tv.get("pad") == pad:
                trip_views.append(json.loads(json.dumps(tv, ensure_ascii=False)))
        trip_views.sort(key=lambda x: str(x.get("trip")))

        association_issues: List[Dict[str, Any]] = []
        for tv in trip_views:
            for issue in tv.get("issues", []):
                if issue.get("blocking", True) and issue.get("status", "open") == "open":
                    association_issues.append(issue)

        # Records in this configured lane that cannot yet be routed are also visible.
        for source in shift["pads"].get(pad, []):
            for chain in self._chains_for_channel(shift_id, source, pad):
                key = _trip_key(chain["current"]["trip"])
                tv = self.trips.get(key)
                if tv is None or tv.get("shift_id") is None or tv.get("pad") is None:
                    association_issues.append(
                        {
                            "code": "unassociated_source_record",
                            "status": "open",
                            "blocking": True,
                            "source": source,
                            "lane": pad,
                            "seq": chain["seq"],
                            "rev": chain["highest_rev"],
                            "trip": chain["current"]["trip"],
                        }
                    )

        # Deduplicate issue IDs/codes.
        seen_issues = set()
        issues = []
        for issue in evidence["issues"] + association_issues:
            ident = issue.get("id") or _canonical(issue)
            if ident not in seen_issues:
                seen_issues.add(ident)
                issues.append(issue)

        confirmed_kg = sum(int(tv.get("confirmed_kg") or 0) for tv in trip_views)
        required_weight = shift["required_weight"]
        evidence_complete = evidence["complete"]
        association_ok = not any(i.get("blocking", True) and i.get("status", "open") == "open" for i in issues)
        can_formal = evidence_complete and association_ok
        threshold_decision = "retain" if confirmed_kg >= required_weight else "release"
        decision = threshold_decision if can_formal else "pending"

        source_positions = []
        for source_ev in evidence["sources"]:
            source_positions.append(
                {
                    "source": source_ev["source"],
                    "lane": pad,
                    "sealed": source_ev["sealed"],
                    "last_seq": source_ev["last_seq"],
                    "received_seqs": source_ev["received_seqs"],
                    "missing_seqs": source_ev["missing_seqs"],
                    "never_produced_seqs": source_ev["never_produced_seqs"],
                    "positions": source_ev["source_positions"],
                }
            )

        return {
            "shift": shift_id,
            "pad": pad,
            "closed": shift_id in self.closed,
            "evidence_complete": evidence_complete,
            "association_ok": association_ok,
            "can_formal": can_formal,
            "complete": can_formal,
            "decision": decision,
            "action": decision,
            "decision_zh": {"retain": "保留", "release": "释放", "pending": "待材料"}[decision],
            "is_retain": decision == "retain",
            "is_release": decision == "release",
            "tentative_decision": threshold_decision,
            "threshold_decision": threshold_decision,
            "confirmed_kg": confirmed_kg,
            "required_weight": required_weight,
            "trip_count": len(trip_views),
            "trips": trip_views,
            "issues": issues,
            "missing_sources": evidence["missing_sources"],
            "source_positions": source_positions,
            "evidence": evidence,
        }

    # -------------------------------------------------------- close/signals
    def _cmd_close(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        shift_id = _require_str(rec, "shift")
        if shift_id not in self.shifts:
            raise NotFoundError(f"未知班次: {shift_id}")
        if shift_id in self.closed:
            return {"status": "duplicate_close", "closed": self.closed[shift_id]}

        self.closed[shift_id] = {"shift": shift_id, "at": at, "order": order}
        result = {"shift": shift_id, "pads": {}}
        for pad in self.shifts[shift_id]["pads"]:
            key = self._key(shift_id, pad)
            self.signals.setdefault(key, [])
            self.feed.setdefault(key, [])
            provisional = self._append_signal(shift_id, pad, at, order, phase="provisional", reason="close")
            # Put the provisional state on client feeds immediately.  If the
            # pad is already complete, the following refresh also appends formal.
            self._route_signal_or_review(provisional, at, order, None)
            formal = self._refresh_key(shift_id, pad, at, order, reason="close-complete")
            result["pads"][pad] = {"provisional": provisional["id"], "formal": formal["id"] if formal else None}
        return result

    close_shift = lambda self, shift_id, at=None: self.submit("close", {"shift": shift_id}, at)  # noqa: E731
    close = close_shift

    def _signature(self, ev: Dict[str, Any]) -> str:
        compact_evidence = []
        for sp in ev["source_positions"]:
            compact_evidence.append(
                (
                    sp["source"],
                    sp["sealed"],
                    sp["last_seq"],
                    tuple(sp["received_seqs"]),
                    tuple(sp["missing_seqs"]),
                    tuple(sp["never_produced_seqs"]),
                )
            )
        trip_sig = []
        for trip in ev["trips"]:
            trip_sig.append(
                (
                    str(trip.get("key")),
                    trip.get("pad"),
                    trip.get("shift_id"),
                    trip.get("confirmed_kg"),
                    tuple((r["source"], r["lane"], r["seq"], r["rev"]) for r in trip.get("source_refs", [])),
                )
            )
        issue_sig = tuple(sorted((i.get("code"), str(i.get("id")), i.get("status", "open")) for i in ev["issues"]))
        return _canonical(
            {
                "can_formal": ev["can_formal"],
                "decision": ev["decision"],
                "tentative": ev["tentative_decision"],
                "kg": ev["confirmed_kg"],
                "evidence": compact_evidence,
                "trips": trip_sig,
                "issues": issue_sig,
            }
        )

    def _append_signal(
        self,
        shift_id: str,
        pad: str,
        at: int,
        order: int,
        *,
        phase: Optional[str] = None,
        reason: str,
        correction: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ev = self.evaluate(shift_id, pad)
        key = self._key(shift_id, pad)
        old = self.current_signal.get(key)
        if phase is None:
            phase = "formal" if ev["can_formal"] else "provisional"

        signal = {
            "id": self._new_id("signal", "sig"),
            "version": len(self.signals.setdefault(key, [])) + 1,
            "shift": shift_id,
            "pad": pad,
            "phase": phase,
            "decision": ev["decision"] if phase == "formal" else "pending",
            "tentative_decision": ev["tentative_decision"],
            "status": "active",
            "claimable": phase == "formal" and ev["decision"] == "release",
            "formal": phase == "formal",
            "temporary": phase == "provisional",
            "stage": phase,
            "action": ev["decision"] if phase == "formal" else "pending",
            "decision_zh": {"retain": "保留", "release": "释放", "pending": "待材料"}[
                ev["decision"] if phase == "formal" else "pending"
            ],
            "is_retain": phase == "formal" and ev["decision"] == "retain",
            "is_release": phase == "formal" and ev["decision"] == "release",
            "at": at,
            "reported_at": at,
            "order": order,
            "reason": reason,
            "signature": self._signature(ev),
            "corrects_signal_id": old["id"] if old else None,
            "correction_id": correction["id"] if correction else None,
            "correction": self._public_ref(correction) if correction else None,
            "confirmed_kg": ev["confirmed_kg"],
            "required_weight": ev["required_weight"],
            "can_formal": ev["can_formal"],
            "evidence_complete": ev["evidence_complete"],
            "missing_sources": ev["missing_sources"],
            "issues": json.loads(json.dumps(ev["issues"], ensure_ascii=False)),
            "source_positions": json.loads(json.dumps(ev["source_positions"], ensure_ascii=False)),
            "trips": json.loads(json.dumps(ev["trips"], ensure_ascii=False)),
        }
        if old is not None:
            old["status"] = "corrected" if correction is not None or (old["phase"] == "formal" and phase != "formal") else "superseded"
            old["corrected_by_signal_id"] = signal["id"]
            if correction is not None:
                old["correction_id"] = correction["id"]
        self.signals[key].append(signal)
        self.current_signal[key] = signal
        return signal

    @staticmethod
    def _public_ref(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        return {k: v for k, v in value.items() if k not in {"affected_keys"}}

    def _refresh_key(
        self,
        shift_id: str,
        pad: str,
        at: int,
        order: int,
        *,
        reason: str,
        correction: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        key = self._key(shift_id, pad)
        if shift_id not in self.closed:
            return None
        ev = self.evaluate(shift_id, pad)
        old = self.current_signal.get(key)
        new_phase = "formal" if ev["can_formal"] else "provisional"
        new_signature = self._signature(ev)

        # A correction changes source revisions and therefore signature.  Avoid
        # duplicate signals when an unrelated channel event does not change view.
        if old is not None and old.get("signature") == new_signature and old.get("phase") == new_phase:
            return old

        signal = self._append_signal(
            shift_id, pad, at, order, phase=new_phase, reason=reason, correction=correction
        )
        self._route_signal_or_review(signal, at, order, correction)
        return signal

    def _active_claims_for_signal(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            c
            for c in self.claims
            if c["shift"] == signal["shift"]
            and c["pad"] == signal["pad"]
            and c["signal_id"] in {signal["id"], signal.get("corrects_signal_id")}
            and c["status"] in {"active", "review_required"}
        ]

    def _route_signal_or_review(
        self, signal: Dict[str, Any], at: int, order: int, correction: Optional[Dict[str, Any]]
    ) -> None:
        key = self._key(signal["shift"], signal["pad"])
        feed = self.feed.setdefault(key, [])

        # Claims against the immediately corrected/current signal need review.
        exact_active = [
            c
            for c in self.claims
            if c["shift"] == signal["shift"]
            and c["pad"] == signal["pad"]
            and c.get("signal_id") == signal.get("corrects_signal_id")
            and c["status"] == "active"
        ]
        unresolved = [
            c
            for c in self.claims
            if c["shift"] == signal["shift"]
            and c["pad"] == signal["pad"]
            and c["status"] in {"active", "review_required"}
        ]
        affected_claims = list(exact_active)
        known_ids = {c["id"] for c in affected_claims}
        if exact_active:
            for claim in unresolved:
                if claim["id"] not in known_ids:
                    affected_claims.append(claim)
                    known_ids.add(claim["id"])
        elif correction is not None:
            # A second correction can arrive while the first review is still open.
            affected_claims = unresolved

        if affected_claims:
            original_fact = None
            if correction is not None:
                original_fact = correction.get("original_start_fact")
            if original_fact is None and signal.get("trips"):
                original_fact = signal["trips"][0].get("start_fact")
            review = {
                "id": self._new_id("review", "review"),
                "status": "pending",
                "shift": signal["shift"],
                "pad": signal["pad"],
                "at": at,
                "order": order,
                "correction_id": correction["id"] if correction else None,
                "caused_by": self._public_ref(correction),
                "original_start_fact": original_fact,
                "signal_id": signal["id"],
                "current_decision": signal["decision"],
                "phase": "correction_review",
                "claim_ids": [c["id"] for c in affected_claims],
                "clients": sorted({c["client"] for c in affected_claims}),
                "responses": [],
            }
            self.reviews.append(review)
            for claim in affected_claims:
                claim["status"] = "review_required"
                claim["review_id"] = review["id"]
                claim.setdefault("review_ids", [])
                if review["id"] not in claim["review_ids"]:
                    claim["review_ids"].append(review["id"])
            item = {
                "feed_seq": len(feed) + 1,
                "type": "correction_review",
                "review_id": review["id"],
                "signal_id": signal["id"],
                "at": at,
                "order": order,
            }
            feed.append(item)
            if correction is not None:
                correction.setdefault("review_ids", []).append(review["id"])
            self._emit("correction_review", review)
            return

        item = {
            "feed_seq": len(feed) + 1,
            "type": "signal",
            "signal_id": signal["id"],
            "at": at,
            "order": order,
        }
        feed.append(item)
        self._emit(f"signal_{signal['phase']}", signal)
        self._emit("signal", signal)

    # ------------------------------------------------------------- clients
    def connect(self, client: str, shift: Optional[str] = None) -> "ClientSession":
        """Return a reconnectable bound interface for one scheduling client."""
        if not isinstance(client, str) or not client:
            raise ValidationError("client 必须是非空字符串")
        return ClientSession(self, client, shift)

    client_session = connect

    def _client_state(self, client: str, shift_id: str, pad: str) -> Dict[str, Any]:
        if shift_id not in self.shifts:
            raise NotFoundError(f"未知班次: {shift_id}")
        if pad not in self.shifts[shift_id]["pads"]:
            raise NotFoundError(f"班次 {shift_id} 未登记卸点 {pad}")
        root = self.clients.setdefault(client, {"keys": {}})
        key = self._key(shift_id, pad)
        state = root["keys"].get(key)
        if state is None:
            state = {
                "client": client,
                "shift": shift_id,
                "pad": pad,
                "key": key,
                "initialized": False,
                "feed_cursor": 0,
                "next_notice": 1,
                "notices": {},
                "pulls": [],
            }
            root["keys"][key] = state
        return state

    def _signal_by_id(self, signal_id: str) -> Optional[Dict[str, Any]]:
        for signals in self.signals.values():
            for signal in signals:
                if signal["id"] == signal_id:
                    return signal
        return None

    def _review_by_id(self, review_id: str) -> Optional[Dict[str, Any]]:
        return next((r for r in self.reviews if r["id"] == review_id), None)

    def _corrections_for_key(self, shift_id: str, pad: str) -> List[Dict[str, Any]]:
        return [
            c
            for c in self.corrections
            if any(a.get("shift") == shift_id and a.get("pad") == pad for a in c.get("affected_keys", []))
        ]

    def _signal_history_brief(self, key: str) -> List[Dict[str, Any]]:
        return [
            {
                "signal_id": s["id"],
                "version": s["version"],
                "phase": s["phase"],
                "decision": s["decision"],
                "status": s["status"],
                "at": s["at"],
                "correction_id": s.get("correction_id"),
                "corrects_signal_id": s.get("corrects_signal_id"),
                "corrected_by_signal_id": s.get("corrected_by_signal_id"),
                "claimable": s.get("claimable"),
            }
            for s in self.signals.get(key, [])
        ]

    def _build_snapshot_notice(self, client: str, shift_id: str, pad: str, at: int, notice_no: int) -> Dict[str, Any]:
        key = self._key(shift_id, pad)
        signal = self.current_signal.get(key)
        key_reviews = [r for r in self.reviews if r["shift"] == shift_id and r["pad"] == pad]
        review = key_reviews[-1] if key_reviews else None
        ev = self.evaluate(shift_id, pad)
        phase = "open"
        if review is not None and review.get("status") == "pending":
            phase = "correction_review"
        elif signal is not None:
            phase = signal["phase"]

        notice: Dict[str, Any] = {
            "notice": notice_no,
            "client": client,
            "shift": shift_id,
            "pad": pad,
            "type": "snapshot",
            "phase": phase,
            "at": at,
            "closed": shift_id in self.closed,
            "signal_id": signal["id"] if signal else None,
            "review_id": review["id"] if review else None,
            "decision": (signal or {}).get("decision", ev["decision"]),
            "tentative_decision": ev["tentative_decision"],
            "claimable": bool(signal and signal.get("claimable") and signal.get("status") == "active"),
            "current_signal": signal,
            "signal": signal,
            "current_evaluation": ev,
            "current": {
                "phase": (signal or {}).get("phase", "open"),
                "decision": (signal or {}).get("decision", ev["decision"]),
                "status": (signal or {}).get("status", "open"),
                "confirmed_kg": ev["confirmed_kg"],
                "required_weight": ev["required_weight"],
                "can_formal": ev["can_formal"],
                "missing_sources": ev["missing_sources"],
                "issues": ev["issues"],
            },
            "history": self._signal_history_brief(key),
            "signal_history": self._signal_history_brief(key),
            "corrected_history": [h for h in self._signal_history_brief(key) if h["status"] in {"corrected", "superseded"}],
            "corrections": self._corrections_for_key(shift_id, pad),
        }
        if review is not None:
            embedded_review = dict(review)
            embedded_review["signal"] = signal
            notice["review"] = embedded_review
            notice["correction_review"] = embedded_review
            notice["original_start_fact"] = review.get("original_start_fact")
            notice["status"] = review.get("status")
        if signal is not None:
            notice["signal_status"] = signal.get("status")
            notice["confirmed_kg"] = signal.get("confirmed_kg")
            notice["required_weight"] = signal.get("required_weight")
            if review is None:
                notice["status"] = signal.get("status")
        return notice

    def _feed_notice(self, state: Dict[str, Any], item: Dict[str, Any], notice_no: int, at: int) -> Dict[str, Any]:
        shift_id, pad = state["shift"], state["pad"]
        if item["type"] == "signal":
            signal = self._signal_by_id(item["signal_id"])
            return {
                "notice": notice_no,
                "client": state["client"],
                "shift": shift_id,
                "pad": pad,
                "type": "signal",
                "phase": signal["phase"],
                "decision": signal["decision"],
                "status": signal["status"],
                "claimable": signal["claimable"] and signal["status"] == "active",
                "signal_id": signal["id"],
                "at": at,
                "signal": signal,
                "history": self._signal_history_brief(state["key"]),
                "corrected_history": [
                    h for h in self._signal_history_brief(state["key"]) if h["status"] in {"corrected", "superseded"}
                ],
            }

        review = self._review_by_id(item["review_id"])
        signal = self._signal_by_id(item["signal_id"])
        review_view = dict(review)
        review_view["signal"] = signal
        return {
            "notice": notice_no,
            "client": state["client"],
            "shift": shift_id,
            "pad": pad,
            "type": "correction_review",
            "phase": "correction_review",
            "decision": signal["decision"] if signal else None,
            "status": review["status"],
            "claimable": bool(signal and signal.get("claimable") and signal.get("status") == "active"),
            "review_id": review["id"],
            "signal_id": signal["id"] if signal else None,
            "at": at,
            "review": review_view,
            "correction_review": review_view,
            "original_start_fact": review.get("original_start_fact"),
            "signal": signal,
            "history": self._signal_history_brief(state["key"]),
            "corrected_history": [
                h for h in self._signal_history_brief(state["key"]) if h["status"] in {"corrected", "superseded"}
            ],
        }

    def pull(
        self,
        client: Any = None,
        shift: Any = None,
        pad: Any = None,
        at: Optional[int] = None,
        order: Optional[int] = None,
    ) -> "PullResult":
        # Accept pull(record), pull(envelope), or pull(client, shift, pad).
        if isinstance(client, dict):
            payload = client.get("record", client)
            at = client.get("at", at) if "record" in client else at
            client = payload.get("client")
            shift = payload.get("shift")
            pad = payload.get("pad")
        if not isinstance(client, str) or not client:
            raise ValidationError("client 必须是非空字符串")
        if not isinstance(shift, str) or not isinstance(pad, str):
            raise ValidationError("pull 需要 shift 和 pad")
        at = self.last_at if at is None else at
        state = self._client_state(client, shift, pad)
        key = state["key"]
        feed = self.feed.setdefault(key, [])

        if not state["initialized"]:
            state["initialized"] = True
            # A late client starts at current state; the snapshot carries correction history.
            state["feed_cursor"] = len(feed)
            notice_no = state["next_notice"]
            payload = self._build_snapshot_notice(client, shift, pad, at, notice_no)
            state["notices"][notice_no] = {
                "notice": notice_no,
                "type": payload["type"],
                "phase": payload["phase"],
                "decision": payload.get("decision"),
                "status": payload.get("status"),
                "signal_id": payload.get("signal_id"),
                "review_id": payload.get("review_id"),
                "acked": False,
                "acked_at": None,
                "payload": payload,
            }
            state["next_notice"] += 1

        for item in feed:
            if item["feed_seq"] <= state["feed_cursor"]:
                continue
            notice_no = state["next_notice"]
            payload = self._feed_notice(state, item, notice_no, at)
            state["notices"][notice_no] = {
                "notice": notice_no,
                "type": payload["type"],
                "phase": payload["phase"],
                "decision": payload.get("decision"),
                "status": payload.get("status"),
                "signal_id": payload.get("signal_id"),
                "review_id": payload.get("review_id"),
                "feed_seq": item["feed_seq"],
                "acked": False,
                "acked_at": None,
                "payload": payload,
            }
            state["feed_cursor"] = item["feed_seq"]
            state["next_notice"] += 1

        pending = [n["payload"] for n in state["notices"].values() if not n["acked"]]
        state["pulls"].append({"at": at, "order": order or self.order, "pending": len(pending)})
        result = PullResult(
            {
                "ok": True,
                "client": client,
                "shift": shift,
                "pad": pad,
                "at": at,
                "notices": pending,
                "pending_count": len(pending),
                "notice": pending[0]["notice"] if pending else None,
                "current": pending[0].get("current") if pending else self._compact_current(shift, pad),
            }
        )
        return result

    poll = pull
    fetch = pull
    fetch_notices = pull
    receive_signals = pull
    pull_signals = pull
    get_notifications = pull

    def _compact_current(self, shift_id: str, pad: str) -> Dict[str, Any]:
        signal = self.current_signal.get(self._key(shift_id, pad))
        ev = self.evaluate(shift_id, pad)
        return {
            "phase": (signal or {}).get("phase", "open"),
            "decision": (signal or {}).get("decision", ev["decision"]),
            "status": (signal or {}).get("status", "open"),
            "can_formal": ev["can_formal"],
            "missing_sources": ev["missing_sources"],
            "confirmed_kg": ev["confirmed_kg"],
        }

    def _cmd_pull(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        client = _require_str(rec, "client")
        shift_id = _require_str(rec, "shift")
        pad = _require_str(rec, "pad")
        return self.pull(client, shift_id, pad, at=at, order=order)

    def ack(
        self,
        client: Any = None,
        shift: Any = None,
        pad: Any = None,
        notice: Any = None,
        at: Optional[int] = None,
    ) -> Dict[str, Any]:
        # Accept ack(record/envelope) in addition to positional arguments.
        if isinstance(client, dict):
            payload = client.get("record", client)
            at = client.get("at", at) if "record" in client else at
            client = payload.get("client")
            shift = payload.get("shift")
            pad = payload.get("pad")
            notice = payload.get("notice")
        at = self.last_at if at is None else at
        state = self._client_state(client, shift, pad)
        if not _is_int(notice):
            raise ValidationError("notice 必须是整数")
        record_notice = state["notices"].get(notice)
        if record_notice is None:
            raise NotFoundError(f"客户端 {client} 没有接到 notice {notice}")
        if record_notice["acked"]:
            return {"status": "duplicate_ack", "client": client, "shift": shift, "pad": pad, "notice": notice}
        record_notice["acked"] = True
        record_notice["acked_at"] = at
        record_notice["acked_order"] = self.order
        return {
            "status": "acked",
            "client": client,
            "shift": shift,
            "pad": pad,
            "notice": notice,
            "phase": record_notice.get("phase"),
            "decision": record_notice.get("decision"),
            "acked_at": at,
        }

    acknowledge = ack
    ack_signal = ack
    ack_notice = ack
    acknowledge_notice = ack

    def _cmd_ack(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        return self.ack(
            _require_str(rec, "client"),
            _require_str(rec, "shift"),
            _require_str(rec, "pad"),
            _require_int(rec, "notice", 1),
            at=at,
        )

    def claim(
        self,
        client: Any = None,
        shift: Any = None,
        pad: Any = None,
        notice: Any = None,
        at: Optional[int] = None,
    ) -> Dict[str, Any]:
        # Accept claim(record/envelope) in addition to positional arguments.
        if isinstance(client, dict):
            payload = client.get("record", client)
            at = client.get("at", at) if "record" in client else at
            client = payload.get("client")
            shift = payload.get("shift")
            pad = payload.get("pad")
            notice = payload.get("notice")
        at = self.last_at if at is None else at
        state = self._client_state(client, shift, pad)
        record_notice = state["notices"].get(notice)
        if record_notice is None:
            raise NotFoundError(f"客户端 {client} 没有接到 notice {notice}")
        if not record_notice["acked"]:
            raise ClaimError("claim 前必须先 ack 该 notice")

        existing = next(
            (c for c in self.claims if c["client"] == client and c["shift"] == shift and c["pad"] == pad and c["notice"] == notice),
            None,
        )
        if existing is not None and existing.get("status") == "active":
            return {"status": "duplicate_claim", "claim": existing}

        signal_id = record_notice.get("signal_id")
        signal = self._signal_by_id(signal_id) if signal_id else None
        key = self._key(shift, pad)
        if signal is None:
            raise ClaimError("该 notice 不包含可认领的正式信号")
        if signal.get("phase") != "formal":
            raise ClaimError("只有处理过正式释放信号才能 claim；该 notice 是 " + str(signal.get("phase")))
        if signal.get("decision") != "release":
            raise ClaimError("只有正式 release 信号可以 claim；当前为 " + str(signal.get("decision")))
        if self.current_signal.get(key, {}).get("id") != signal["id"] or signal.get("status") != "active":
            raise ClaimError("该释放信号已被纠正或取代，不是当下有效的释放信号")

        claim = {
            "id": self._new_id("claim", "claim"),
            "client": client,
            "shift": shift,
            "pad": pad,
            "notice": notice,
            "signal_id": signal["id"],
            "at": at,
            "order": self.order,
            "status": "active",
            "review_id": record_notice.get("review_id"),
        }
        self.claims.append(claim)
        record_notice["claim_id"] = claim["id"]

        review_id = record_notice.get("review_id")
        if review_id:
            review = self._review_by_id(review_id)
            if review is not None:
                review.setdefault("responses", []).append(
                    {"client": client, "notice": notice, "claim_id": claim["id"], "at": at, "action": "claimed_current_release"}
                )
                if signal["decision"] == "release":
                    review["status"] = "disposed"
                    review["disposed_at"] = at
                    review["disposition_claim_id"] = claim["id"]
        return {"status": "claimed", "claim": claim}

    claim_pad = claim
    claim_release = claim
    claim_notice = claim
    take_over = claim

    def _cmd_claim(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        return self.claim(
            _require_str(rec, "client"),
            _require_str(rec, "shift"),
            _require_str(rec, "pad"),
            _require_int(rec, "notice", 1),
            at=at,
        )

    # ------------------------------------------------------------- inspect
    def client_activity(self, shift_id: Optional[str] = None, pad: Optional[str] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for client, root in self.clients.items():
            for state in root["keys"].values():
                if shift_id is not None and state["shift"] != shift_id:
                    continue
                if pad is not None and state["pad"] != pad:
                    continue
                for notice_no, item in state["notices"].items():
                    claim = next(
                        (
                            c
                            for c in self.claims
                            if c["client"] == client
                            and c["shift"] == state["shift"]
                            and c["pad"] == state["pad"]
                            and c["notice"] == notice_no
                        ),
                        None,
                    )
                    rows.append(
                        {
                            "client": client,
                            "shift": state["shift"],
                            "pad": state["pad"],
                            "notice": notice_no,
                            "type": item.get("type"),
                            "phase": item.get("phase"),
                            "decision": item.get("decision"),
                            "status": item.get("status"),
                            "acked": item["acked"],
                            "acked_at": item.get("acked_at"),
                            "processed_as": (
                                f"{item.get('phase')}_{item.get('decision')}"
                                if item.get("phase") in {"formal", "provisional"} and item.get("decision")
                                else item.get("phase")
                            ),
                            "claim_id": claim["id"] if claim else item.get("claim_id"),
                            "claim_status": claim["status"] if claim else None,
                            "signal_id": item.get("signal_id"),
                            "review_id": item.get("review_id"),
                        }
                    )
        rows.sort(key=lambda r: (r["shift"], r["pad"], r["client"], r["notice"]))
        return rows

    downstream_status = client_activity
    processed_notices = client_activity

    def pending_reviews(self, shift: Optional[str] = None, pad: Optional[str] = None) -> List[Dict[str, Any]]:
        return [
            review
            for review in self.reviews
            if (shift is None or review["shift"] == shift)
            and (pad is None or review["pad"] == pad)
            and review.get("status") == "pending"
        ]

    def correction_review_index(self) -> List[Dict[str, Any]]:
        """Answer which correction forced which claimed site into review."""
        rows = []
        for review in self.reviews:
            corr = next((c for c in self.corrections if c["id"] == review.get("correction_id")), None)
            rows.append(
                {
                    "correction_id": review.get("correction_id"),
                    "review_id": review["id"],
                    "shift": review["shift"],
                    "pad": review["pad"],
                    "status": review["status"],
                    "claimed_by_clients": review.get("clients", []),
                    "claim_ids": review.get("claim_ids", []),
                    "original_start_fact": review.get("original_start_fact"),
                    "correction": corr,
                }
            )
        return rows

    def _public_chain(self, chain: Dict[str, Any]) -> Dict[str, Any]:
        return json.loads(json.dumps(chain, ensure_ascii=False, default=str))

    def pad_inspect(self, shift_id: str, pad: str) -> Dict[str, Any]:
        ev = self.evaluate(shift_id, pad)
        key = self._key(shift_id, pad)
        return {
            "shift": shift_id,
            "pad": pad,
            "phase": (self.current_signal.get(key) or {}).get("phase", "open"),
            "decision": (self.current_signal.get(key) or {}).get("decision", ev["decision"]),
            "can_formal": ev["can_formal"],
            "missing_sources": ev["missing_sources"],
            "evaluation": ev,
            "missing_evidence": {
                "missing_sources": ev["missing_sources"],
                "sources": ev["evidence"]["sources"],
            },
            "current_signal": self.current_signal.get(key),
            "signals": self.signals.get(key, []),
            "signal_history": self._signal_history_brief(key),
            "feed": self.feed.get(key, []),
            "source_records": [
                self._public_chain(c)
                for c in self.chains
                if c.get("shift_id") == shift_id and c.get("lane") == pad
            ],
            "reviews": [r for r in self.reviews if r["shift"] == shift_id and r["pad"] == pad],
            "claims": [c for c in self.claims if c["shift"] == shift_id and c["pad"] == pad],
            "client_activity": self.client_activity(shift_id, pad),
            "downstream": self.client_activity(shift_id, pad),
        }

    def inspect(self, shift: Optional[Any] = None, pad: Optional[str] = None) -> Dict[str, Any]:
        if isinstance(shift, dict):
            payload = shift.get("record", shift)
            shift = payload.get("shift")
            pad = payload.get("pad", pad)
        if shift is not None and not isinstance(shift, str):
            raise ValidationError("shift 必须是字符串")
        if shift is not None:
            if shift not in self.shifts:
                raise NotFoundError(f"未知班次: {shift}")
            if pad is not None:
                return self.pad_inspect(shift, pad)
            return {
                "shift": self.shifts[shift],
                "closed": self.closed.get(shift),
                "pads": {name: self.pad_inspect(shift, name) for name in self.shifts[shift]["pads"]},
                "missing_evidence": self.missing_evidence(shift),
                "source_records": [self._public_chain(c) for c in self.chains if c.get("shift_id") == shift],
                "trips": [tv for tv in self.trips.values() if tv.get("shift_id") == shift],
                "corrections": [
                    c for c in self.corrections if any(a.get("shift") == shift for a in c.get("affected_keys", []))
                ],
                "reviews": [r for r in self.reviews if r["shift"] == shift],
                "claims": [c for c in self.claims if c["shift"] == shift],
                "client_activity": self.client_activity(shift),
                "downstream": self.client_activity(shift),
                "correction_review_index": [
                    row for row in self.correction_review_index() if row["shift"] == shift
                ],
                "issues": [i for i in self.issues if i.get("shift") == shift or not i.get("shift")],
            }

        if pad is not None:
            matches = {}
            for sid, config in self.shifts.items():
                if pad in config["pads"]:
                    matches[sid] = self.pad_inspect(sid, pad)
            return {"pad": pad, "shifts": matches, "client_activity": self.client_activity(pad=pad), "downstream": self.client_activity(pad=pad)}

        return {
            "shifts": list(self.shifts.values()),
            "source_records": [self._public_chain(c) for c in self.chains],
            "trips": list(self.trips.values()),
            "closed": self.closed,
            "corrections": self.corrections,
            "reviews": self.reviews,
            "claims": self.claims,
            "clients": {client: root["keys"] for client, root in self.clients.items()},
            "client_activity": self.client_activity(),
            "downstream": self.client_activity(),
            "correction_review_index": self.correction_review_index(),
            "pending_reviews": self.pending_reviews(),
            "issues": self.issues,
            "missing_evidence": self.missing_evidence(),
        }

    query = inspect
    inspect_shift = inspect
    snapshot = inspect
    get_state = inspect
    dump_state = inspect
    get_missing_evidence = missing_evidence

    def signals_for(self, shift: str, pad: Optional[str] = None) -> Any:
        """Return signal history for one shift/pad, or all pads in a shift."""
        if pad is not None:
            return list(self.signals.get(self._key(shift, pad), []))
        if shift not in self.shifts:
            raise NotFoundError(f"未知班次: {shift}")
        return {pad_name: list(self.signals.get(self._key(shift, pad_name), [])) for pad_name in self.shifts[shift]["pads"]}

    def list_reviews(self, shift: Optional[str] = None, pad: Optional[str] = None) -> List[Dict[str, Any]]:
        return [
            review
            for review in self.reviews
            if (shift is None or review["shift"] == shift) and (pad is None or review["pad"] == pad)
        ]

    def _cmd_inspect(self, rec: Dict[str, Any], at: int, order: int) -> Dict[str, Any]:
        shift_id = rec.get("shift")
        pad = rec.get("pad")
        if shift_id is not None and not isinstance(shift_id, str):
            raise ValidationError("shift 必须是字符串")
        if pad is not None and not isinstance(pad, str):
            raise ValidationError("pad 必须是字符串")
        return self.inspect(shift=shift_id, pad=pad)


class PullResult(dict):
    """Dict response that also supports ``result[0]`` for single-batch callers."""

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self["notices"][key]
        return super().__getitem__(key)

    def __len__(self) -> int:
        return len(self["notices"])

    def __iter__(self):
        return iter(self["notices"])

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class ClientSession:
    """Convenience same-interface reconnect handle for one schedule client."""

    def __init__(self, bus: "EventBus", client: str, shift: Optional[str] = None) -> None:
        self.bus = bus
        self.client = client
        self.shift = shift

    def _resolve(self, shift_or_pad: str, pad: Optional[str]) -> Tuple[str, str]:
        if pad is None:
            if self.shift is None:
                raise ValidationError("必须提供 shift 和 pad")
            return self.shift, shift_or_pad
        return shift_or_pad, pad

    def pull(self, shift_or_pad: str, pad: Optional[str] = None, at: Optional[int] = None) -> PullResult:
        shift_id, pad_name = self._resolve(shift_or_pad, pad)
        return self.bus.pull(self.client, shift_id, pad_name, at=at)

    fetch = pull
    poll = pull

    def ack(self, shift_or_pad: str, pad: Any = None, notice: Optional[int] = None, at: Optional[int] = None) -> Dict[str, Any]:
        # Support both ack(shift, pad, notice) and shift-bound ack(pad, notice).
        if notice is None:
            if not _is_int(pad):
                raise ValidationError("notice 必须是整数")
            shift_id, pad_name = self._resolve(shift_or_pad, None)
            notice = pad
        else:
            shift_id, pad_name = self._resolve(shift_or_pad, pad)
        return self.bus.ack(self.client, shift_id, pad_name, notice, at=at)

    acknowledge = ack

    def claim(self, shift_or_pad: str, pad: Any = None, notice: Optional[int] = None, at: Optional[int] = None) -> Dict[str, Any]:
        if notice is None:
            if not _is_int(pad):
                raise ValidationError("notice 必须是整数")
            shift_id, pad_name = self._resolve(shift_or_pad, None)
            notice = pad
        else:
            shift_id, pad_name = self._resolve(shift_or_pad, pad)
        return self.bus.claim(self.client, shift_id, pad_name, notice, at=at)

    def activity(self, shift: Optional[str] = None, pad: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = self.bus.client_activity(shift, pad)
        return [row for row in rows if row["client"] == self.client]


ShiftEventBus = EventBus
ShiftBus = EventBus
Bus = EventBus
create_bus = EventBus
EventBusClient = ClientSession
ScheduleClient = ClientSession


def replay_file(path: Any, bus: Optional[EventBus] = None) -> EventBus:
    """Replay one JSONL scenario file into an :class:`EventBus`."""
    from pathlib import Path

    path = Path(path)
    bus = bus or EventBus()
    with path.open("r", encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                bus.handle(event)
            except (json.JSONDecodeError, BusError) as exc:
                raise BusError(f"{path.name} 第 {line_no} 行: {exc}") from exc
    return bus


process_file = replay_file
run_scenario = replay_file


def process_events(events: Iterable[Dict[str, Any]], bus: Optional[EventBus] = None) -> EventBus:
    """Functional helper: process envelopes into a new or supplied bus."""
    return (bus or EventBus()).replay_events(events)


def _main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import sys
    from pathlib import Path

    parser = argparse.ArgumentParser(description="运行矿区班次事件总线演练")
    parser.add_argument("paths", nargs="+", type=Path, help="JSONL 文件或包含 JSONL 的目录")
    parser.add_argument("--quiet", action="store_true", help="只输出最终摘要")
    args = parser.parse_args(argv)

    files: List[Any] = []
    for path in args.paths:
        if path.is_dir():
            files.extend(sorted(path.glob("*.jsonl")))
        else:
            files.append(path)

    for file_path in files:
        bus = EventBus()
        action_outputs = []
        with file_path.open("r", encoding="utf-8") as stream:
            for line_no, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                event = json.loads(line)
                result = bus.handle(event)
                if event["kind"] in {"pull", "inspect", "ack", "claim"}:
                    # Freeze command results before later corrections mutate claims.
                    action_outputs.append(json.loads(json.dumps(result, ensure_ascii=False, default=str)))
        print(f"# {file_path}", file=sys.stderr)
        if not args.quiet:
            for output in action_outputs:
                print(json.dumps(output, ensure_ascii=False, default=str))
        summary = {
            "file": str(file_path),
            "shifts": list(bus.shifts),
            "reviews": [
                {"id": r["id"], "shift": r["shift"], "pad": r["pad"], "status": r["status"], "correction_id": r["correction_id"]}
                for r in bus.reviews
            ],
            "claims": [
                {"id": c["id"], "client": c["client"], "shift": c["shift"], "pad": c["pad"], "status": c["status"]}
                for c in bus.claims
            ],
            "missing_evidence": {
                sid: {pad: ev["missing_sources"] for pad, ev in pads.items()}
                for sid, pads in bus.missing_evidence().items()
            },
        }
        print(json.dumps({"summary": summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

