# Companion role runtime v1 candidate

Schema and examples mirror the Platform candidate in this isolated task. They
are proposed exchange contracts, not published root contracts. Companion is
the sole owner of role state, persona revision copy and the immutable turn
snapshot. Only the registered `platform` service credential may call
`/internal/v1/role-runtime/manage` with `operation:apply`; list is available
to that same credential. Each apply has an exact `expected_version` and stable
`request_id`. A retry returns the original result. A profile version conflict
is refused before changing the persona or role.

`application_id` binds a pause and enable to one immutable configuration
signature. Enable may only use that application's exact disabled snapshot;
Platform supplies the authenticated console principal as `operator` so Persona
approval and publication history attribute the application to its initiator.
It reuses the published persona and source profile revision even if the source
profile's version advances during a Memory retry. A new dynamic target starts
unpublished and applies its profile through the normal draft, approval, and
publication ledger in the same transaction as the role fact and receipt.
Target extensions survive application, while absent optional profile text is
cleared. Replaying a receipt adds no second publication.

A Platform cancellation advances the current Core version to disabled using
the pinned role/profile fact. It does not re-read a deleted or changed source
profile. A later old enable receipt returns its original result without
restoring live bindings.

The list also exposes existing deployment roles that have not been managed yet.
An explicit apply for one of those actor IDs may pass null profile ID/version;
it retains the published persona revision and original binding. Disabling an
adopted role blocks new work without deleting its persona, history, or static
binding declaration. An old enable receipt cannot reactivate a later disabled
role. No existing deployment role is adopted automatically.

The existing Companion database holds role metadata and operation receipts in
its metadata table. The deployment must enable `personas`, register a distinct
Platform caller credential, configure `provider_self_service` and its
`provider_selector`, and keep the existing origin, Memory, Gateway and sender
services under their own credentials. Back up the Companion database together
with the coordinated Platform and Memory units before enabling new roles.
