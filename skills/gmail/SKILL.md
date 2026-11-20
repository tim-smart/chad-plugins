---
name: gmail
description: "Gmail: draft or send."
---

**Draft.** Compose in chat by default; use `gmailCreateDraft` only when explicitly asked to save a Gmail draft.

**Send.** Confirm the recipient first, then call `gmailPreview`. If the owner approves or cancels in chat instead of clicking, direct them to **Approve & send** or **Cancel**; edits can replace the preview directly via `gmailPreview`. If a send is not confirmed, check Sent before posting another preview.

**Validation.** When draft or preview validation fails on owner-supplied text, explain what failed and ask rather than rewriting it.
