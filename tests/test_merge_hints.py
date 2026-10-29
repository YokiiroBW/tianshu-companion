import asyncio
from types import SimpleNamespace

import pytest

from tianshu_companion.clients import Memory
from tianshu_companion.contracts import Fault


@pytest.mark.parametrize("change", [None, "owner", "record", "score"])
def test_internal_hints_are_bound_to_the_frozen_owner_record_whitelist(change):
    first = dict(record_id="r1", record_version=1)
    second = dict(record_id="r2", record_version=2)
    window = dict(targets=[dict(scope=dict(person_id="p1"), known_memories=[first, second])])
    hint = dict(subject_person_id="p1", new=second, prior=first, score=0.98)
    if change == "owner":
        hint["subject_person_id"] = "p2"
    if change == "record":
        hint["prior"] = dict(record_id="other", record_version=1)
    if change == "score":
        hint["score"] = float("nan")

    async def call(path, request, **kwargs):
        assert path == "/internal/v1/memory/candidates/hints"
        return dict(job_id="job", batch_ref="batch", merge_candidates=[hint])

    client = object.__new__(Memory)
    client.client = SimpleNamespace(call=call)
    if change:
        with pytest.raises(Fault):
            asyncio.run(client.segment_merge_hints("job", "batch", window))
    else:
        assert asyncio.run(client.segment_merge_hints("job", "batch", window)) == [hint]
