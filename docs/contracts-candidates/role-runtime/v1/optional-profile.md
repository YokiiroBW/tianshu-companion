# Optional persona profiles

`profile_id` and `profile_version` may both be null for creation or editing. They must be null together. Name remains required. No new fields or permissions are introduced.

A new runtime actor without a profile receives its own immutable, approved baseline revision (clear, accurate replies), with no copied character fields, relationship facts or memory. It is disabled unless the operator explicitly enables dialogue. Profile selection can be added later; selecting none on a non-legacy role publishes a fresh baseline without profile extensions. Existing deployment roles retain their own published persona when the profile is omitted. Actor identity and existing memory boundaries never change.

The existing nullable schema is unchanged. Role creation, cancel/retry and later edits retain request identity, compare-and-swap versions and atomic publication.
