"""Regression tests for evidence-aware import and retrieval/use separation."""

import json

import pytest

from import_memory import ImportEngine, chunk_turns, detect_and_parse


@pytest.mark.asyncio
async def test_retrieval_does_not_refresh_decay_but_touch_records_use(bucket_mgr):
    bucket_id = await bucket_mgr.create(content="retrieval use separation")
    before = (await bucket_mgr.get(bucket_id))["metadata"]
    before_active = before["last_active"]
    before_count = before["activation_count"]

    await bucket_mgr.record_retrieval(bucket_id, source="test")
    retrieved = (await bucket_mgr.get(bucket_id))["metadata"]

    assert retrieved["retrieval_count"] == 1
    assert retrieved["last_retrieval_source"] == "test"
    assert retrieved["last_active"] == before_active
    assert retrieved["activation_count"] == before_count
    assert retrieved["used_count"] == 0

    await bucket_mgr.record_association(bucket_id, source="write_then_recall")
    associated = (await bucket_mgr.get(bucket_id))["metadata"]
    assert associated["retrieval_count"] == 2
    assert associated["association_count"] == 1
    assert associated["activation_count"] == before_count
    assert associated["last_active"] == before_active

    await bucket_mgr.record_use(bucket_id, source="breath_search")
    used = (await bucket_mgr.get(bucket_id))["metadata"]

    assert used["retrieval_count"] == 3
    assert used["activation_count"] == before_count + 1
    assert used["used_count"] == 1
    assert used["last_use_source"] == "breath_search"
    assert used["last_used_at"] == used["last_active"]

    report = bucket_mgr.ledger_integrity_report()
    assert report["trace_catalog_projection"]["unknown_event_count"] == 0


@pytest.mark.asyncio
async def test_create_persists_occurrence_provenance_and_relations(bucket_mgr):
    bucket_id = await bucket_mgr.create(
        content="a memory with evidence",
        occurred_at="2026-07-15T20:00:00",
        provenance={
            "kind": "conversation_import",
            "source_platform": "chatgpt",
            "conversation_ids": ["conv-1"],
            "message_ids": ["m1", "m2"],
            "channels": ["user_visible", "assistant_visible"],
        },
        relations=[
            {
                "bucket_id": "older-bucket",
                "type": "evolution",
                "score": 88.23456,
                "source": "history_import",
            }
        ],
    )

    metadata = (await bucket_mgr.get(bucket_id))["metadata"]
    assert metadata["occurred_at"] == "2026-07-15T20:00:00"
    assert metadata["provenance"]["conversation_ids"] == ["conv-1"]
    assert metadata["provenance"]["channels"] == [
        "user_visible",
        "assistant_visible",
    ]
    assert metadata["relations"] == [
        {
            "bucket_id": "older-bucket",
            "type": "evolution",
            "source": "history_import",
            "score": 88.2346,
        }
    ]


def test_chatgpt_parser_and_chunk_keep_separate_channels_and_evidence():
    raw = json.dumps(
        [
            {
                "id": "conv-1",
                "mapping": {
                    "node-u": {
                        "message": {
                            "id": "msg-u",
                            "author": {"role": "user"},
                            "create_time": 1,
                            "content": {"content_type": "text", "parts": ["hello"]},
                        }
                    },
                    "node-a": {
                        "message": {
                            "id": "msg-a",
                            "author": {"role": "assistant"},
                            "create_time": 2,
                            "content": {"content_type": "text", "parts": ["hi"]},
                        }
                    },
                    "node-t": {
                        "message": {
                            "id": "msg-t",
                            "author": {"role": "tool"},
                            "create_time": 3,
                            "content": {"content_type": "text", "parts": ["result"]},
                        }
                    },
                },
            }
        ]
    )

    turns = detect_and_parse(raw, "conversations.json")
    assert [turn["channel"] for turn in turns] == [
        "user_visible",
        "assistant_visible",
        "tool_result",
    ]
    assert [turn["message_id"] for turn in turns] == ["msg-u", "msg-a", "msg-t"]
    assert all(turn["conversation_id"] == "conv-1" for turn in turns)

    chunk = chunk_turns(turns, target_tokens=1000, human_label="Duoduo")[0]
    assert chunk["channels"] == [
        "user_visible",
        "assistant_visible",
        "tool_result",
    ]
    assert chunk["message_ids"] == ["msg-u", "msg-a", "msg-t"]
    assert chunk["conversation_ids"] == ["conv-1"]
    assert "[Duoduo] [user_visible|msg:msg-u|conv:conv-1] hello" in chunk["content"]
    assert "[工具结果] [tool_result|msg:msg-t|conv:conv-1] result" in chunk["content"]


class _SimilarBucketManager:
    def __init__(self):
        self.created = []

    def find_exact_content(self, content, domain_filter=None):
        return None

    async def search(self, query, limit=3, domain_filter=None):
        return [
            {
                "id": "old-bucket",
                "content": "an older related memory",
                "score": 92.0,
                "metadata": {
                    "domain": ["relationship"],
                    "tags": [],
                    "importance": 5,
                    "valence": 0.5,
                    "arousal": 0.3,
                },
            }
        ]

    async def create(self, **kwargs):
        self.created.append(kwargs)
        return "new-bucket"

    async def update(self, bucket_id, **kwargs):
        raise AssertionError("exact_only import must not semantic-merge")


class _NoMergeDehydrator:
    api_available = True

    async def merge(self, old, new):
        raise AssertionError("exact_only import must not call LLM merge")


@pytest.mark.asyncio
async def test_import_defaults_to_relation_edge_instead_of_semantic_merge(tmp_path):
    manager = _SimilarBucketManager()
    engine = ImportEngine(
        {"buckets_dir": str(tmp_path), "human": "Duoduo"},
        manager,
        _NoMergeDehydrator(),
    )
    engine.state.data["source_file"] = "history.json"
    engine.state.data["source_hash"] = "abc123"
    item = {
        "name": "new turn",
        "content": "a newer related memory",
        "domain": ["relationship"],
        "tags": [],
        "importance": 6,
        "valence": 0.6,
        "arousal": 0.4,
    }
    chunk = {
        "timestamp_start": "2026-07-15T20:00:00",
        "timestamp_end": "2026-07-15T20:01:00",
        "platforms": ["chatgpt"],
        "conversation_ids": ["conv-1"],
        "message_ids": ["m1", "m2"],
        "channels": ["user_visible", "assistant_visible"],
    }

    merged = await engine._merge_or_create_item(item, chunk)

    assert merged is False
    assert len(manager.created) == 1
    created = manager.created[0]
    assert created["occurred_at"] == "2026-07-15T20:00:00"
    assert created["provenance"]["message_ids"] == ["m1", "m2"]
    assert created["relations"] == [
        {
            "bucket_id": "old-bucket",
            "type": "related",
            "score": 92.0,
            "source": "history_import",
        }
    ]


def test_association_has_weak_capped_ranking_bonus_without_full_touch():
    from bucket_scoring import calc_touch_score

    base = calc_touch_score({"activation_count": 1, "association_count": 0})
    linked = calc_touch_score({"activation_count": 1, "association_count": 3})
    many_links = calc_touch_score({"activation_count": 1, "association_count": 999})

    assert base < linked < 0.2
    assert many_links == calc_touch_score(
        {"activation_count": 1, "association_count": 5}
    )


def test_association_gives_small_decay_bonus_without_resetting_time(decay_eng):
    from datetime import datetime

    common = {
        "importance": 5,
        "activation_count": 1,
        "last_active": datetime.now().isoformat(),
        "arousal": 0.3,
        "type": "dynamic",
    }
    base = decay_eng.calculate_score({**common, "association_count": 0})
    linked = decay_eng.calculate_score({**common, "association_count": 3})
    capped = decay_eng.calculate_score({**common, "association_count": 999})

    assert linked > base
    assert linked < base * 1.11
    assert capped == decay_eng.calculate_score({**common, "association_count": 5})
