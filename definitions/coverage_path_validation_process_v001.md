# Coverage Analysis and Path Validation in Large-Scale Networks

## Introduction

This validation engine is only **one subsystem** inside a much larger digital-twin program for semiconductor fabs.
At the moment our network model stores the bare minimum—nodes, links, utilities—but **lacks rich metadata** such as:

* **Equipment class**  *(PROCESSING / PRODUCTION / SUPPLY)*
* **Flow direction**  *(IN / OUT or PUSH / PULL)*

As soon as those attributes land in the database, we will extend the validator with class-aware rules (e.g. *process-gas must never back-feed into supply lines*) and flow-based simulations.

---

## Phase 1 – Sampling & Path Discovery

| Step                     | What happens                                                             | **Why it matters**                                     |
| ------------------------ | ------------------------------------------------------------------------ | ------------------------------------------------------ |
| 1. **Toolset scope**     | Load universe of nodes & links for the chosen **toolset**.               | Keeps runtime bounded; mirrors maintenance boundaries. |
| 2. **Random PoC pair**   | Uniformly pick two *Points-of-Contact* on different pieces of equipment. | Uncovers anomalies scripted tests often miss.          |
| 3. **Coverage tracking** | Update bitarrays to monitor cumulative **coverage %**.                   | Guarantees even exploration; exposes cold zones.       |
| 4. **Path finding**      | Call `nw_downstream()` (Dijkstra-style) to extract a candidate path.     | Follows real-world flow direction.                     |
| 5. **No-path handling**  | Store the failed attempt as a **review item**.                           | Missing connectivity is itself a defect.               |

---

## Phase 2 – Incremental Validations

| Validation                                           | What it checks                                          | **Why**                                                       |
| ---------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------- |
| **Connectivity**                                     | Gap-free node & link sequence.                          | Detects broken GUID mappings or cleansing side-effects.       |
| **Utility Consistency**                              | `utility_no` stays constant until authorised to change. | Enforces media segregation (e.g., exhaust vs. process gases). |
| *(Future)* **Flow / Material / Diameter / Pressure** | Requires richer metadata.                               | Will unlock compliance and safety rules.                      |

---

## Phase 3 – Findings & Knowledge Capture

| Bucket                | Stored in table        | **Why separate?**                                                       |
| --------------------- | ---------------------- | ----------------------------------------------------------------------- |
| **Reviews**           | `tb_run_reviews`       | Human confirmation where logic is insufficient or context is ambiguous. |
| **Validation Errors** | `tb_validation_errors` | Deterministic rule failures, versioned for SLA dashboards.              |
| **Tags**              | `tb_path_tags`         | Lightweight labels to enrich analytics & ML training.                   |

---

## Error Taxonomy — Components & Classification

To keep the rule engine extensible we classify every finding along **five orthogonal dimensions**.

| Dimension            | Enum              | Key Values                                                      | Example                                             |
| -------------------- | ----------------- | --------------------------------------------------------------- | --------------------------------------------------- |
| **Validation Scope** | `ValidationScope` | `CONNECTIVITY`, `UTILITY`, `FLOW`, `MATERIAL`, `QA`, `SCENARIO` | Utility mismatch ⇒ scope = `UTILITY`                |
| **Severity**         | `Severity`        | `INFO`, `WARNING`, `ERROR`, `CRITICAL`                          | Broken exhaust path in cleanroom ⇒ `CRITICAL`       |
| **Validation Type**  | `ValidationType`  | `STRUCTURAL`, `LOGICAL`, `PERFORMANCE`, `COMPLIANCE`            | Path too long for pump spec ⇒ `PERFORMANCE`         |
| **Object Type**      | `ObjectType`      | `NODE`, `LINK`, `POC`, `SEGMENT`, `PATH`                        | Mis-typed utility on a link ⇒ object\_type = `LINK` |
| **Error Type**       | `ErrorType`       | 80 + granular codes (see table below)                           | Utility mismatch ⇒ `UTILITY_MISMATCH`               |

### Selected `ErrorType` codes

| Category                       | Codes                                                           |
| ------------------------------ | --------------------------------------------------------------- |
| **Not-found / missing**        | `NOT_FOUND_NODE`, `MISSING_GUID`, `MISSING_FLOW`                |
| **Continuity / connectivity**  | `BROKEN_CONTINUITY`, `DISCONNECTED_LINK`, `CONNECTING_GAP`      |
| **Mismatch / incompatibility** | `UTILITY_MISMATCH`, `MATERIAL_MISMATCH`, `FLOW_DIRECTION_ISSUE` |
| **Invalid / wrong data**       | `INVALID_NODE`, `INVALID_LENGTH`, `WRONG_UTILITY`               |
| **Anomalies**                  | `UNUSUAL_LENGTH`, `HIGH_COMPLEXITY`, `CIRCULAR_LOOP_DETECTED`   |

> **Why so many codes?**
> Fine-grained types let dashboards trend each failure class separately, help the triage bot suggest fixes, and feed balanced training data to future AI models.

---

## Next Step — Scenario-Based Validation

Random sampling checks **inside one toolset**.
Critical flows—exhaust, vacuum, slurry—span multiple levels, mezzanines, and chase-downs.

1. **Authoring** – Engineers define start/end PoCs for each scenario.
2. **Execution loop** – Re-validate the same path after *every* cleansing cycle or fix ticket.
3. **Why re-run a path?**

   * Cleansing scripts can silently break connectivity.
   * A fix on node A may introduce a regression on link B.
   * Trendline KPIs (latency, hop-count) warn of slow drift.

---

## Lessons Learned

* **Random ≠ naïve** – Bias rules (utility diversity, min distance) are essential for signal over noise.
* **Profile first** – On 200 k + PoCs a single extra loop costs minutes; bit-ops save hours.
* **Data gravity** – Logging *all* attempts (success + failure) yields a training goldmine.

---

## Future Directions

1. **Richer metadata ingestion** (equipment class, flow direction, pressure class).
2. **Simulation hooks** – Feed validated graphs to CFD / pressure-drop solvers.
3. **AI-assisted triage** – Fine-tune LLMs on `tb_validation_errors` + human resolutions for auto-suggestions.

---
