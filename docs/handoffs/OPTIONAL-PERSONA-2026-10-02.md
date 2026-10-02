# Optional persona role creation

Based only on deployed rc.2. New roles require a name; a reusable persona profile is optional. No profile gives the new actor a fixed minimal baseline, never a household/profile copy. The default UI saves the actor disabled. Later profile binding and clearing preserve actor identity. Old deployment personas remain intact.

Validation: Platform 11 focused tests; Companion 50 persona/role tests; TypeScript typecheck and Vite build; actual loopback TLS Platform/Companion/Memory/Gateway with Chromium: no-profile creation, optional none with profiles, persistence after reload, attach and clear, no model requests. Existing role, cancellation and idempotency regression included in the focused suite. No production credentials or data in fixtures.

Only Platform and Companion need new images. No schema migration, permission or credential changes. Back up the resident authority and controls before release; retain rc.2 images and restore point.
