import json
from pathlib import Path
import unittest

from event_bus import BusError, ClaimError, EventBus, replay_file


ROOT = Path(__file__).parent


def emit(bus, at, kind, **record):
    return bus.handle({"at": at, "kind": kind, "record": record})


def basic_shift(bus, shift="S", start=0, end=20, pads=("A", "B"), weight=10):
    return emit(
        bus,
        0,
        "shift",
        shift=shift,
        starts=start,
        ends=end,
        pads={pad: ["vehicle", "scale"] for pad in pads},
        required_weight=weight,
    )


def seal_both(bus, at, shift, pad, vehicle=1, scale=1, vehicle_gaps=None, scale_gaps=None):
    emit(bus, at, "seal", shift=shift, pad=pad, source="vehicle", last_seq=vehicle, gaps=vehicle_gaps or [])
    emit(bus, at, "seal", shift=shift, pad=pad, source="scale", last_seq=scale, gaps=scale_gaps or [])


class ExampleScenarioTest(unittest.TestCase):
    def test_two_pads_and_late_weigh_are_assigned_to_observed_shift(self):
        bus = replay_file(ROOT / "examples" / "two-pads-one-late-weigh.jsonl")
        a = bus.inspect("D", "A")
        b = bus.inspect("D", "B")

        self.assertEqual(a["current_signal"]["phase"], "formal")
        self.assertEqual(a["current_signal"]["decision"], "retain")
        self.assertEqual(a["current_signal"]["confirmed_kg"], 13)
        self.assertEqual(b["current_signal"]["decision"], "release")
        self.assertEqual(b["current_signal"]["confirmed_kg"], 5)
        self.assertEqual(a["evaluation"]["missing_sources"], [])

        schedule = bus.pull("schedule", "D", "B", at=20)
        self.assertEqual(schedule[0]["phase"], "formal")
        self.assertEqual(schedule[0]["decision"], "release")

    def test_correction_after_claim_creates_review_and_late_client_history(self):
        bus = replay_file(ROOT / "examples" / "change-pad-after-claim.jsonl")

        ops_after = bus.pull("ops", "E", "B", at=20)
        self.assertEqual(ops_after[0]["type"], "correction_review")
        self.assertEqual(ops_after[0]["original_start_fact"]["pad"], "A")
        self.assertEqual(ops_after[0]["decision"], "retain")

        backup = bus.pull("backup-late", "E", "B", at=21)
        notice = backup[0]
        self.assertEqual(notice["notice"], 1)
        self.assertEqual(notice["phase"], "correction_review")
        self.assertTrue(any(h["decision"] == "release" and h["status"] == "corrected" for h in notice["history"]))
        self.assertEqual(bus.reviews[0]["clients"], ["ops"])
        self.assertEqual(bus.claims[0]["status"], "review_required")

        activity = {(r["client"], r["notice"]): r for r in bus.client_activity("E", "B")}
        self.assertEqual(activity[("ops", 1)]["processed_as"], "formal_release")
        self.assertTrue(activity[("ops", 1)]["acked"])


class TripAndSealTest(unittest.TestCase):
    def test_weigh_arriving_before_load_follows_start_observation(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 1, "weigh", source="scale", seq=1, rev=1, trip="t", kg=12, lane="A")
        # Before the load there is no interpretable shift attribution.
        self.assertTrue(any(i["code"] == "missing_start_fact" for i in bus.issues) is False)
        emit(bus, 2, "load", source="vehicle", seq=1, rev=1, trip="t", observed_at=3, pad="A", lane="A")
        tv = bus.trips["str:t"]
        self.assertEqual(tv["shift_id"], "S")
        self.assertEqual(tv["observed_at"], 3)
        self.assertEqual(tv["confirmed_kg"], 12)

        emit(bus, 20, "close", shift="S")
        seal_both(bus, 21, "S", "A")
        signal = bus.current_signal[bus._key("S", "A")]
        self.assertEqual(signal["phase"], "formal")
        self.assertEqual(signal["decision"], "retain")

    def test_no_seal_is_not_absence_but_explicit_gap_is(self):
        bus = EventBus()
        basic_shift(bus, pads=("A",))
        emit(bus, 1, "load", source="vehicle", seq=1, rev=1, trip="t", observed_at=1, pad="A", lane="A")
        emit(bus, 20, "close", shift="S")

        missing = bus.evidence_for_pad("S", "A")
        self.assertEqual(set(missing["missing_sources"]), {"vehicle", "scale"})

        emit(bus, 21, "seal", shift="S", pad="A", source="vehicle", last_seq=2, gaps=[[2, 2]])
        vehicle = bus.evidence_for_source("S", "A", "vehicle")
        self.assertEqual(vehicle["missing_seqs"], [])
        self.assertEqual(vehicle["never_produced_seqs"], [2])
        self.assertTrue(vehicle["complete"])

        # An undeclared missing seq 1 when last_seq=2 is not complete.
        emit(bus, 22, "seal", shift="S", pad="A", source="scale", last_seq=2, gaps=[])
        scale = bus.evidence_for_source("S", "A", "scale")
        self.assertEqual(scale["missing_seqs"], [1, 2])
        self.assertFalse(scale["complete"])

    def test_declared_empty_channel_is_complete(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 20, "close", shift="S")
        emit(bus, 21, "seal", shift="S", pad="B", source="vehicle", last_seq=0, gaps=[])
        emit(bus, 21, "seal", shift="S", pad="B", source="scale", last_seq=0, gaps=[])
        ev = bus.evaluate("S", "B")
        self.assertTrue(ev["can_formal"])
        self.assertEqual(ev["decision"], "release")

    def test_one_incomplete_pad_does_not_lock_other_pad(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 1, "load", source="vehicle", seq=1, rev=1, trip="a", observed_at=1, pad="A", lane="A")
        emit(bus, 2, "weigh", source="scale", seq=1, rev=1, trip="a", kg=1, lane="A")
        emit(bus, 20, "close", shift="S")
        seal_both(bus, 21, "S", "B", vehicle=0, scale=0)

        b = bus.current_signal[bus._key("S", "B")]
        a = bus.current_signal[bus._key("S", "A")]
        self.assertEqual(b["phase"], "formal")
        self.assertEqual(b["decision"], "release")
        self.assertEqual(a["phase"], "provisional")
        self.assertIn("vehicle", a["missing_sources"])


class RevisionConflictTest(unittest.TestCase):
    def test_same_rev_conflict_blocks_until_higher_rev_and_stale_rev_does_not_overwrite(self):
        bus = EventBus()
        basic_shift(bus, pads=("A",))
        emit(bus, 0, "load", source="vehicle", seq=1, rev=1, trip="t", observed_at=1, pad="A", lane="A")
        emit(bus, 1, "weigh", source="scale", seq=1, rev=1, trip="t", kg=11, lane="A")
        conflict = emit(bus, 2, "weigh", source="scale", seq=1, rev=1, trip="t", kg=99, lane="A")
        self.assertEqual(conflict["status"], "conflict")
        emit(bus, 20, "close", shift="S")
        emit(bus, 21, "seal", shift="S", pad="A", source="vehicle", last_seq=1)
        emit(bus, 21, "seal", shift="S", pad="A", source="scale", last_seq=1)
        self.assertFalse(bus.evaluate("S", "A")["can_formal"])

        stale = emit(bus, 22, "weigh", source="scale", seq=1, rev=0 if False else 1, trip="t", kg=5, lane="A")
        # rev=1 is the conflicted current rev; another variant remains a conflict.
        self.assertEqual(stale["status"], "conflict")

        resolved = emit(bus, 23, "weigh", source="scale", seq=1, rev=2, trip="t", kg=12, lane="A")
        self.assertEqual(resolved["status"], "accepted")
        ev = bus.evaluate("S", "A")
        self.assertTrue(ev["can_formal"])
        self.assertEqual(ev["confirmed_kg"], 12)

        late_old = emit(bus, 24, "weigh", source="scale", seq=1, rev=1, trip="t", kg=999, lane="A")
        self.assertEqual(late_old["status"], "stale_revision")
        self.assertEqual(bus.evaluate("S", "A")["confirmed_kg"], 12)

    def test_correction_keeps_original_channel_sequence(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 1, "load", source="vehicle", seq=7, rev=1, trip="m", observed_at=1, pad="A", lane="A")
        emit(bus, 2, "load", source="vehicle", seq=7, rev=2, trip="m", observed_at=1, pad="B", lane="A")
        chain = bus.chains[0]
        self.assertEqual((chain["lane"], chain["seq"]), ("A", 7))
        self.assertEqual(bus.trips["str:m"]["pad"], "B")
        vehicle_a = bus.evidence_for_source("S", "A", "vehicle")
        self.assertEqual(vehicle_a["received_seqs"], [7])


class ClientSignalTest(unittest.TestCase):
    def _ready_release_b(self, bus):
        emit(bus, 1, "load", source="vehicle", seq=1, rev=1, trip="b", observed_at=1, pad="B", lane="B")
        emit(bus, 2, "weigh", source="scale", seq=1, rev=1, trip="b", kg=1, lane="B")
        emit(bus, 20, "close", shift="S")
        seal_both(bus, 21, "S", "B")

    def test_provisional_ack_is_not_claimable_but_formal_is(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 20, "close", shift="S")
        seal_both(bus, 21, "S", "B", vehicle=0, scale=0)

        first = bus.pull("ops", "S", "B", at=22)
        # Snapshot starts at the current formal state; provisional history is retained.
        self.assertEqual(first[0]["phase"], "formal")
        bus.ack("ops", "S", "B", 1, at=23)
        claim = bus.claim("ops", "S", "B", 1, at=24)
        self.assertEqual(claim["status"], "claimed")

    def test_provisional_notice_ack_is_recorded_but_not_claimable(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 20, "close", shift="S")
        provisional = bus.pull("ops", "S", "A", at=21)
        self.assertEqual(provisional[0]["phase"], "provisional")
        bus.ack("ops", "S", "A", 1, at=22)
        with self.assertRaises(ClaimError):
            bus.claim("ops", "S", "A", 1, at=23)
        activity = bus.client_activity("S", "A")
        self.assertEqual(activity[0]["processed_as"], "provisional_pending")

    def test_existing_client_gets_close_provisional_on_reconnect(self):
        bus = EventBus()
        basic_shift(bus)
        client = bus.connect("ops", "S")
        before = client.pull("A", at=10)
        self.assertEqual(before[0]["phase"], "open")
        client.ack("A", 1, at=11)

        emit(bus, 20, "close", shift="S")
        after = client.pull("A", at=21)
        self.assertTrue(any(n["phase"] == "provisional" for n in after))

    def test_client_session_reconnect_uses_same_ack_state(self):
        bus = EventBus()
        basic_shift(bus)
        emit(bus, 20, "close", shift="S")
        seal_both(bus, 21, "S", "A", vehicle=0, scale=0)
        client = bus.connect("ops", "S")
        first = client.pull("A", at=22)
        self.assertEqual(first[0]["phase"], "formal")
        client.ack("A", 1, at=23)
        reconnected = bus.connect("ops", "S")
        self.assertEqual(reconnected.pull("A", at=24)["pending_count"], 0)

    def test_claim_requires_ack_and_current_formal_release(self):
        bus = EventBus()
        basic_shift(bus)
        self._ready_release_b(bus)
        pulled = bus.pull("ops", "S", "B", at=22)
        with self.assertRaises(ClaimError):
            bus.claim("ops", "S", "B", pulled[0]["notice"], at=23)
        bus.ack("ops", "S", "B", 1, at=24)
        self.assertEqual(bus.claim("ops", "S", "B", 1, at=25)["status"], "claimed")

        emit(bus, 26, "load", source="vehicle", seq=2, rev=1, trip="heavy", observed_at=2, pad="A", lane="A")
        emit(bus, 27, "weigh", source="scale", seq=2, rev=1, trip="heavy", kg=50, lane="A")
        emit(bus, 28, "load", source="vehicle", seq=2, rev=2, trip="heavy", observed_at=2, pad="B", lane="A")
        bus.pull("ops", "S", "B", at=29)
        # Old notice cannot be claimed after correction.
        with self.assertRaises(ClaimError):
            bus.claim("ops", "S", "B", 1, at=30)

    def test_unclaimed_pad_receives_new_current_decision(self):
        bus = EventBus()
        basic_shift(bus)
        self._ready_release_b(bus)
        first = bus.pull("listener", "S", "B", at=22)
        bus.ack("listener", "S", "B", first[0]["notice"], at=23)

        emit(bus, 24, "load", source="vehicle", seq=2, rev=1, trip="heavy", observed_at=2, pad="A", lane="A")
        emit(bus, 25, "weigh", source="scale", seq=2, rev=1, trip="heavy", kg=50, lane="A")
        emit(bus, 26, "load", source="vehicle", seq=2, rev=2, trip="heavy", observed_at=2, pad="B", lane="A")

        second = bus.pull("listener", "S", "B", at=27)
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["type"], "signal")
        self.assertEqual(second[0]["phase"], "formal")
        self.assertEqual(second[0]["decision"], "retain")
        self.assertEqual(bus.reviews, [])

    def test_source_positions_are_snapshotted_on_formal_signal(self):
        bus = EventBus()
        basic_shift(bus, pads=("A",))
        emit(bus, 1, "load", source="vehicle", seq=1, rev=1, trip="t", observed_at=1, pad="A", lane="A")
        emit(bus, 2, "weigh", source="scale", seq=1, rev=1, trip="t", kg=12, lane="A")
        emit(bus, 20, "close", shift="S")
        seal_both(bus, 21, "S", "A")
        signal = bus.current_signal[bus._key("S", "A")]
        sources = {(p["source"], p["lane"]): p for p in signal["source_positions"]}
        self.assertTrue(sources[("vehicle", "A")]["sealed"])
        self.assertEqual(sources[("scale", "A")]["last_seq"], 1)
        self.assertEqual(signal["source_positions"][0]["positions"][0]["status"], "received")


class CliAndContractTest(unittest.TestCase):
    def test_reader_kinds_are_all_supported(self):
        kinds = set(json.loads((ROOT / "examples" / "message_types.json").read_text(encoding="utf-8")))
        bus = EventBus()
        for kind in kinds:
            self.assertTrue(hasattr(bus, f"_cmd_{kind}"), kind)

    def test_unknown_shift_pull_raises_not_found(self):
        bus = EventBus()
        with self.assertRaises(BusError):
            bus.pull("ops", "NOPE", "A")


if __name__ == "__main__":
    unittest.main()
