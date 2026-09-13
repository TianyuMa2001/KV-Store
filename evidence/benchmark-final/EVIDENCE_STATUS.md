# Withdrawn benchmark evidence

These measurements are retained for audit only and must not be cited as current verified results.

The legacy case 10 (W=5 with one allegedly unreachable peer) returned 201 for all 298 measured writes in repetition 0. The harness terminated a launch process without verifying that the peer's listening port was actually down. Oracle javapath may spawn a separate JVM; the old harness recorded neither server PID nor effective quorum configuration. Therefore the claimed fault was not established.

The recorded JAR SHA-256 is a7ee3a23a6b51b4874532fd3f72eec915f6603e34e9250e163ef21f0ca506472. Another local packaged artifact had SHA-256 3d75b614df58317180f01a00dece0bb3eb44404b0dc45d427bff83b466d91bcc and different controller bytecode, illustrating stale-build ambiguity. A different archive hash alone is not proof of a source-code defect.

Use evidence/benchmark-verified instead. The corrected harness builds from current source, launches the direct JVM, verifies server PID and effective settings, proves port-level unreachability, checks write quorum responses, and refuses incomplete shutdown or artifact changes.
