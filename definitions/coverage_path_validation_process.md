# Coverage Analysis and Path Validation in Large‑Scale Networks


## Introduction
Validating connectivity and utility integrity in sprawling industrial networks—such as those in modern semiconductor fabs—requires an **incremental, data‑driven workflow**.  
The approach below grew out of real‑world pain‑points: incomplete CAD imports, millions of nodes & links, and a constant stream of cleansing / fix tickets.  
The three‑phase pipeline we use today lets us *discover*, *verify*, and finally *interpret* network behaviour at scale, while leaving room for continuous improvement.

> **Why start with an incremental design?**  
> Because every run teaches us something new about bad data, edge‑cases, or performance bottlenecks. Lock‑step iterations keep runtimes predictable and feedback loops tight.


## Phase&nbsp;1 – Sampling & Path Discovery
| Step | What happens | **Why it matters** |
|------|--------------|--------------------|
| 1. Toolset scope | Choose a **toolset** (logical slice of the fab) and compute its universe of nodes & links. | Limits runtime and aligns findings with maintenance boundaries. |
| 2. Random PoC pair | Uniformly sample two *Points‑of‑Contact* (PoCs) from different pieces of equipment. | Randomness surfaces hidden anomalies that scripted tests miss. |
| 3. Coverage tracking | Maintain a bit‑array for every node/link to monitor cumulative **coverage %**. | Guarantees we explore the network evenly and spot cold zones early. |
| 4. Shortest‑path search | Invoke `nw_downstream()` / Dijkstra to fetch a candidate path. | Mirrors the real flow direction engineers care about. |
| 5. No‑path handling | If no path exists, record a **candidate for review** instead of discarding. | Missing connectivity is itself a data defect worth surfacing. |


## Phase&nbsp;2 – Incremental Validations
After a path is found we layer quick, stateless checks first, then costlier, context‑heavy ones.

| Validation | What it checks | **Why** |
|------------|----------------|---------|
| **Connectivity** | Every node & link in sequence exists, no gaps, no duplicates. | Catches broken GUID mappings or cleansing side‑effects early. |
| **Utility Consistency** | `utility_no` stays constant until a device authorised to mix/change utilities appears. | Guarantees media segregation (e.g., exhaust vs. process gases) required by safety codes. |

Additional validators (flow direction, pressure classes, diameter transitions, etc.) plug into the same interface as the data matures.


## Phase&nbsp;3 – Findings & Knowledge Capture
Results from Phase 2 feed three buckets:

| Bucket | Stored in table | **Why separate?** |
|--------|-----------------|-------------------|
| **Reviews** | `tb_run_reviews` | Some anomalies need human eyes—e.g., ambiguous overlaps the rules can’t disambiguate. |
| **Validation Errors** | `tb_validation_errors` | Deterministic rule failures with severity & scope allow dashboards and SLA alerts. |
| **Tags** | `tb_path_tags` | Lightweight labels (QA, RISK, INS, CRIT…) enrich paths for analytics & ML training. |

Below is a quick field‑by‑field rationale for each table.

### 3.1 `tb_path_tags`
* `tag_type`, `tag_code` – Enables faceted search (e.g., *CRIT_HIGH*).  
* `source` – Distinguishes **SYSTEM** auto‑tags from **USER** annotations.  
* `confidence` – Lets the UI fade uncertain inferences to prompt review.

### 3.2 `tb_validation_errors`
* `severity` & `error_type` – Drive alert routing (P1 vs P3).  
* `object_*` columns – Jump straight from log to offending entity in the 3‑D viewer.  
* `error_data` (JSON) – Stores rule‑specific context without schema churn.

### 3.3 `tb_run_reviews`
* `flag_type` – Groups manual QA, performance outliers, anomalies.  
* `status` workflow – Tracks triage → fix → verification lifecycle.  
* `assigned_to` – Encourages accountability in large teams.


## Next Step – Scenario‑Based Validation
Random sampling is powerful, **but it looks only inside a single toolset**.  
Critical flows—*exhaust, vacuum, chemical slurries*—span multiple levels and chase‑downs across the entire fab.  
A **scenario** bundles one or more *pre‑defined* critical paths:

1. **Authoring** – A process engineer records start/end PoCs that define the scenario.  
2. **Execution loop** – The engine tests the same path *every* time data is cleansed or a fix ticket closes.  
3. **Why re‑test the same path?**  
   * Cleansing scripts can *silently* break connectivity.  
   * Fixes to one defect may introduce another (regression risk).  
   * KPI trendlines (latency, node count) reveal slow drift before it explodes.

> **Objective**: guarantee that life‑safety routes (exhaust), or production‑critical lines (CMP slurry), remain intact 24 × 7—even while the spatial DB keeps evolving.


## Lessons Learned
* **Random ≠ naïve** – Bias rules (utility diversity, min distance) are essential to extract signal.  
* **Profile first** – On 200 k+ PoCs a single extra loop costs minutes; vectorised bit‑ops save hours.  
* **Data gravity** – Capturing *all* attempts (success + failure) builds a goldmine for future AI triage helpers.


## Future Directions
1. **Richer metadata** – Bring in equipment classes, flow direction, and sensor limits.  
2. **Simulation integration** – Feed validated graphs to CFD / pressure‑drop solvers.  
3. **AI‑assisted triage** – Fine‑tune LLMs on `tb_validation_errors` explanations + operator resolutions.
