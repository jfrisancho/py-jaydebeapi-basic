# managers/run.py

import sys
from datetime import datetime
from db import Database
from string_helper import StringHelper
from managers.coverage import CoverageManager
from managers.random import RandomSamplingManager
from managers.path import PathManager
from managers.validation import ValidationManager

from models import RunConfig, RandomRunConfig, RunStatus, RandomRunSummary

def run_random_sampling(random_config: RandomRunConfig, verbose_mode: bool = False):
    db = Database()
    try:
        run_id = f'RAND_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        run_tag = random_config.tag or run_id

        # Create RunConfig
        run_cfg = RunConfig(
            run_id=run_id,
            approach='RANDOM',
            method='SIMPLE',
            started_at=datetime.now(),
            tag=run_tag,
            random_config=random_config,
            verbose_mode=verbose_mode,
        )

        # 1. Init coverage and managers
        coverage_mgr = CoverageManager(db, run_cfg)
        path_mgr = PathManager(db, run_cfg)
        random_mgr = RandomSamplingManager(db, run_cfg)
        validation_mgr = ValidationManager(db, run_cfg)

        # 2. Store initial run record
        _store_run_metadata(db, run_cfg, status=RunStatus.INITIALIZED)

        total_attempts = 0
        total_paths_found = 0
        unique_paths = set()

        # 3. Sampling loop
        while not coverage_mgr.has_met_coverage_target():
            total_attempts += 1

            # a. Select random poc pair (returns (start, end) info or None)
            poc_pair = random_mgr.fetch_random_poc_pair()
            if not poc_pair:
                # No more pairs or not enough candidates
                break

            # b. Attempt pathfinding between pocs
            path = path_mgr.fetch_path_between_pocs(*poc_pair)
            if path and path.nodes:
                # c. Update coverage
                is_new_coverage = coverage_mgr.update_coverage(path)
                if is_new_coverage:
                    unique_paths.add(path.hash)
                    total_paths_found += 1
                    path_mgr.store_path_result(path, run_id)
                else:
                    # Path already covered, skip storing
                    pass
            else:
                # d. If not found, and both pocs are marked as 'used', flag for review
                random_mgr.handle_missing_path(poc_pair, run_id)

            # Progress print
            if verbose_mode:
                pct = coverage_mgr.current_coverage_pct()
                print(f'Attempt {total_attempts} - Coverage: {pct:.2f}%')

        # 4. Finalize and validate
        achieved_coverage = coverage_mgr.current_coverage_pct()
        _store_run_metadata(db, run_cfg, status=RunStatus.SAMPLING_COMPLETED, total_paths_found=total_paths_found, achieved_coverage=achieved_coverage)

        validation_results = validation_mgr.validate_sampled_paths(run_id)
        # Final run summary
        _store_run_metadata(db, run_cfg, status=RunStatus.COMPLETED, total_paths_found=total_paths_found, achieved_coverage=achieved_coverage, validation_results=validation_results)

        # Optionally print summary
        if verbose_mode:
            print(f'\nRandom sampling run completed.\nTotal attempts: {total_attempts}\nTotal paths: {total_paths_found}\nCoverage: {achieved_coverage:.2f}%')
            print(f'Validation Results: {validation_results}')

    finally:
        db.close()

def _store_run_metadata(db, run_cfg, status, total_paths_found=0, achieved_coverage=0.0, validation_results=None):
    # Only insert if not exists, otherwise update status/metrics
    sql = '''
        INSERT INTO tb_runs (id, date, approach, method, coverage_target, fab_no, phase_no, model_no, toolset,
            total_coverage, total_nodes, total_links, tag, status, run_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            status = ?,
            total_coverage = ?,
            total_nodes = ?,
            total_links = ?,
            tag = ?
    '''
    params = [
        run_cfg.run_id,
        datetime.now().date(),
        run_cfg.approach,
        run_cfg.method,
        run_cfg.random_config.coverage_target,
        run_cfg.random_config.fab_no,
        run_cfg.random_config.phase_no,
        run_cfg.random_config.model_no,
        run_cfg.random_config.toolset,
        achieved_coverage,
        0,  # total_nodes: CoverageManager will update later
        0,  # total_links: CoverageManager will update later
        run_cfg.tag,
        status.value,
        datetime.now(),
        # For update:
        status.value, achieved_coverage, 0, 0, run_cfg.tag
    ]
    db.update(sql, params)

if __name__ == '__main__':
    # Example entrypoint: construct RandomRunConfig from CLI or config file
    import argparse
    parser = argparse.ArgumentParser(description='Random path validation run')
    parser.add_argument('--fab', type=str, help='Building code')
    parser.add_argument('--model', type=str, help='Data model type (BIM or 5D)')
    parser.add_argument('--phase', type=str, help='Phase (P1, P2, A, B)')
    parser.add_argument('--toolset', type=str, help='Toolset code')
    parser.add_argument('--coverage_target', type=float, required=True, help='Target node+link coverage [0,1]')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    random_cfg = RandomRunConfig(
        coverage_target=args.coverage_target,
        fab=args.fab,
        model=args.model,
        phase=args.phase,
        toolset=args.toolset,
    )
    run_random_sampling(random_cfg, verbose_mode=args.verbose)
