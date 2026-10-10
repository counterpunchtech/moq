# [S] TS compliance duration-fidelity captures the whole source

## Goal

Interop's TS-compliance `duration-fidelity` check (`test/ts/run.sh`) captures
the whole round-tripped stream, not a fraction of it, fixed at its cause.

## Plan

Seen 2026-10-10 on #5158's second Interop run: the export captured 3.3 s of a
20.1 s source, and the check failed; it also failed once on #5140 and passed
on rerun. Planned under this questline's cause-first rules (decided
2026-10-10): find whether the export ends early (an announce `End`, linger, or
a catalog finish arriving before the media), starts late, or the capture is
cut off by the harness. Fix that at its source, with a regression that fails
without it. No longer timeout and no lower duration threshold.

Check against #5147 (export ts follows announcements), which may change how an export ends.

Public API: none expected. Wire: none expected.

Investigation (2026-10-10):

On 2026-10-10, an isolated `nix develop --command just test ts` against
main at `6559065b8` captured 20.47 s of the 20.06 s source (ratio 1.021).
The publisher exited 0 and duration-fidelity passed. The run failed a separate
T-STD check (PID 257 held over one second); its subscriber log also contained
missed-deadline drops. That does not reproduce the reported 3.3 s capture.

The capture remains bounded by the existing harness timeout, and no duration
threshold or timeout was changed. #5147 still owns the export announcement/end
lifecycle, while #5175 owns groups left in flight when a relay track ends.
A truncating capture with both process exit statuses and relay/publisher/
subscriber logs is still needed to distinguish these paths before a fix.
