Why Scenario-Based Validation Is Essential (and Random Alone Isn’t Enough)

Aspect	Pure Random Sampling	Scenario-Driven Validation	Why Scenarios Matter

Scope of search	Constrained to a single toolset universe (≈ 103 k nodes & 104 k links in your B-phase example).	Spans multiple toolsets, phases, and models (BIM/5D; P1–P2; A–B), following real production routes.	Critical flows—exhaust, vacuum, slurry—always cross toolset boundaries; random never touches them.
Path logic	Stops at the first valid connectivity path, regardless of utility purpose or flow direction.	Starts from engineer-selected PoCs and follows a pre-approved, domain-meaningful route (e.g., only pumping lines, only exhaust).	Lets you verify compliance rules (e.g., no back-feed from production to supply) that random cannot express.
Risk coverage	Delivers statistical insight but may miss low-probability, high-impact faults.	Guarantees inspection of every life-safety or production-critical path, every run.	Safety audits and ISO/IEC standards demand proof that all critical lines are intact.
Regression testing	Cannot deterministically re-test the same path after a DB cleansing or hot-fix.	Re-validates identical paths after each cleansing cycle or ticket close.	Confirms that fixes stay fixed and new scripts don’t silently break legacy routes.
Performance	1 % coverage → ~520 s (4-10 paths) 	10 % coverage → ~2 h 30 m (≈ 115 paths).	One scenario path typically executes in seconds; dozens run in minutes.
Trend analytics	Coverage %, success/failure ratios—useful but coarse.	Per-scenario KPIs (latency, hop-count, pressure drop).	Detects gradual drift (e.g., path length creeping up) before it becomes an outage.
Human interpretability	Engineers must reverse-engineer why a random path even exists.	Path meaning is documented up-front; reviewers know exactly what equipment pair is under test.	Cuts review time, reduces cognitive load, aids training of new staff.



---

Key Arguments in Favour of Scenarios

1. Guarantee Coverage of Must-Not-Fail Lines
Fire-exhaust, toxic-gas, slurry-return, pump-down lines are single-points-of-failure. A random engine that finds only 4–10 paths at 1 % coverage may never sample them.


2. Embed Domain Knowledge
Path semantics—processing → scrubber, supply → tool → exhaust—cannot be encoded in random PoC selection. Scenarios transform tacit expert knowledge into reproducible tests.


3. Enable True Regression Testing
Cleansing scripts fix thousands of GUIDs in bulk. Only scenarios let you re-run the exact path afterwards to verify that:

connectivity is restored,

utility consistency still holds,

no new mismatches emerged.



4. Support Compliance and Audit Trails
Regulators often ask: “Show me proof that your hazardous-gas exhaust path is continuous on every weekly update.”
A timestamped scenario run with VALIDATION_PASS is defensible evidence; random statistics aren’t.


5. Operational Efficiency
Running 50 scenarios nightly (each < 5 s) is cheaper than chasing 10 % random coverage (> 2 h). You reserve the heavy random runs for exploratory health checks, while scenarios guard the crown jewels.


6. Data Quality Feedback Loop
Scenario failures point directly to the assets that matter. Operators fix those first, accelerating mean-time-to-repair and building trust in the validation system.




---

Recommended Hybrid Strategy

1. Fast Guards (Scenarios)
Every commit / nightly build

• Validate 100 % of predefined critical paths.*



2. Exploratory Scans (Random)
Weekly or on-demand

• Run 1 %–10 % coverage to surface unknown issues, tune bias rules, and grow the scenario library.*



3. Performance Roadmap

Short-term:⁣ SQL tuning & loop refactors to hit 300 s for 1 % coverage.

Mid-term:⁣ Threaded or async execution to keep < 5 min wall-time even at 10 % coverage.

Long-term:⁣ Distributed workers (Python + queue) to parallelise per-toolset runs across the cluster.




By layering deterministic scenarios atop probabilistic random sampling, you get the best of both worlds: iron-clad coverage where it counts and continuing discovery everywhere else.






Coverage Analysis and Path Validation in Large-Scale Networks

Introduction

Validating connectivity and utility integrity in sprawling industrial networks—such as those found in modern semiconductor fabs—demands an incremental, data-driven workflow.
Our three-phase pipeline lets us discover problems, verify rules, and interpret results at scale, all while keeping feedback loops tight.

> Why an incremental approach?
Every run exposes new bad-data patterns, edge-cases, or performance bottlenecks. Iterating in small, observable steps makes fixes cheaper and progress measurable.




---

Phase 1 – Sampling & Path Discovery

Step	What happens	Why it matters

1. Toolset scope	Load universe of nodes & links for the chosen toolset.	Keeps runtime bounded; mirrors maintenance boundaries.
2. Random PoC pair	Uniformly pick two Points-of-Contact on different pieces of equipment.	Uncovers anomalies scripted tests often miss.
3. Coverage tracking	Update bit-arrays to monitor cumulative coverage %.	Ensures even exploration; exposes cold zones.
4. Path finding	Call nw_downstream() (Dijkstra-style) to extract a candidate path.	Follows real-world flow direction.
5. No-path handling	Store the failed attempt as a review item.	Missing connectivity is itself a defect.


Performance reference: On 8 toolsets in the B-phase of 5D (~103 k nodes & 104 k links)
* • 1 % target coverage* → ≈ 520 s runtime, finds 4 – 10 paths
* • 10 % target coverage* → ≈ 2 h 30 m runtime, finds ≈ 115 paths

Optimization goal: Bring 1 % coverage down to ≤ 300 s via query tuning & loop refactors, then introduce multithreaded execution to keep higher-coverage runs under 5 minutes.


---

Phase 2 – Incremental Validations

Validation	What it checks	Why

Connectivity	Gap-free node & link sequence.	Detects broken GUID mappings or cleansing side-effects.
Utility Consistency	utility_no stays constant until authorised to change.	Enforces media segregation (e.g., exhaust vs. process gases).
(Future) Flow / Material / Diameter / Pressure	Requires richer metadata (equipment class, flow direction, etc.).	Enables compliance and safety rules.



---

Phase 3 – Findings & Knowledge Capture

Bucket	Stored in table	Why separate?

Reviews	tb_run_reviews	Human confirmation where logic is insufficient or context is ambiguous.
Validation Errors	tb_validation_errors	Deterministic rule failures, versioned for SLA dashboards.
Tags	tb_path_tags	Lightweight labels to enrich analytics & ML training.


Error Taxonomy — Components & Classification

Every finding is labelled along five orthogonal axes:

1. Validation Scope – CONNECTIVITY, UTILITY, FLOW, MATERIAL, QA, SCENARIO


2. Severity – INFO, WARNING, ERROR, CRITICAL


3. Validation Type – STRUCTURAL, LOGICAL, PERFORMANCE, COMPLIANCE


4. Object Type – NODE, LINK, POC, SEGMENT, PATH


5. Error Type – 80 + granular codes (e.g., UTILITY_MISMATCH, BROKEN_CONTINUITY, UNUSUAL_LENGTH)



Fine-grained typing enables precise dashboards, targeted alerting, and well-balanced AI training sets.


---

Scenario-Based Validation — Closing the Gaps Left by Random Sampling

Why Random Alone Fails

Aspect	Pure Random Sampling	Scenario-Driven Validation	Why Scenarios Matter

Scope	Confined to one toolset universe.	Spans multiple toolsets, phases, and models.	Critical flows (exhaust, vacuum, slurry) always cross boundaries.
Path logic	Accepts the first connectivity-valid route.	Follows engineer-approved, domain-meaningful paths (e.g., “only pumping lines”).	Enables compliance and process-safety checks.
Risk coverage	Statistical—it may skip low-probability, high-impact faults.	Deterministically covers all life-safety / production-critical lines.	Regulators demand proof these paths are intact.
Regression testing	Cannot replay identical paths post-cleansing.	Re-validates the same path after each DB cleanse or fix ticket.	Confirms that fixes stay fixed and new scripts don’t break legacy routes.
Performance	1 % → ~520 s; 10 % → ~2 h 30 m.	Dozens of scenario paths run in seconds.	Fast gating for CI/CD pipelines on spatial-DB updates.
Trend analytics	Coverage %, pass/fail ratios.	Per-scenario KPIs (latency, hop-count, pressure drop).	Detects gradual drift before it becomes an outage.
Human interpretability	Reviewers must divine what the random path represents.	Path intent is documented up-front.	Cuts review time, aids onboarding.


Key Arguments for Scenarios

1. Guarantee Coverage of Must-Not-Fail Lines – Random runs at 1 % coverage may never hit the toxic-gas exhaust path.


2. Embed Domain Knowledge – Scenarios codify tacit expert insight (process → scrubber, supply → tool → exhaust).


3. True Regression Testing – Replay identical paths after mass-cleansing to verify no silent regressions.


4. Audit & Compliance – Timestamped scenario passes serve as defensible evidence for safety authorities.


5. Operational Efficiency – Fast, deterministic checks run on every commit; random scans run weekly for discovery.


6. Faster MTTR – Scenario failures point straight to the assets that matter, accelerating repairs.



Recommended Hybrid Strategy

Layer	Cadence	Purpose

Fast guards (scenarios)	Every commit / nightly	Certify 100 % of predefined critical paths.
Exploratory scans (random)	Weekly / on demand	Surface unknown issues, tune bias rules, grow scenario library.


Performance Roadmap

1. Short-term – Reduce 1 % coverage runtime from 520 s → ≤ 300 s (SQL tuning & loop refactors).


2. Mid-term – Add multithreading / asyncio to keep 10 % coverage runs under 5 min.


3. Long-term – Distribute per-toolset jobs across a worker cluster for near-real-time validation.




---

Future Directions

1. Richer metadata ingestion (equipment class, flow direction, pressure class).


2. Simulation hooks – Feed validated graphs to CFD / pressure-drop solvers.


3. AI-assisted triage – Fine-tune LLMs on tb_validation_errors + human resolutions for auto-suggested fixes.

