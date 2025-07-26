- correctly computes HH:MM:SS (and falls back to MM:SS) using divmod,
- fixes the undefined duration_mmss and uses the dynamically chosen label/value,
- minimizes repeated attribute lookups,
- uses only single-quoted string literals,
- and leans on integer arithmetic (faster in CPython 3.11):

```python
def print_sampling_statistics(
    self,
    metrics: SamplingMetrics,
    coverage_stats: dict[str, Any],
    elapsed_time: float,
    config: RunConfig
) -> dict[str, Any]:
    '''Print comprehensive sampling statistics and return calculated metrics.'''

    # Localize for speed
    total_attempts = metrics.total_attempts
    total_paths = metrics.total_paths_found
    failed = getattr(metrics, 'failed_attempts', total_attempts - total_paths)

    # Success / failure rates
    success_rate = (total_paths / total_attempts * 100) if total_attempts else 0.0
    failure_rate = (failed / total_attempts * 100) if total_attempts else 0.0

    # Throughput
    paths_per_second = (total_paths / elapsed_time) if elapsed_time else 0.0
    attempts_per_second = (total_attempts / elapsed_time) if elapsed_time else 0.0
    attempts_per_path = (total_attempts / total_paths) if total_paths else float('inf')

    # Coverage
    target_cov = config.coverage_target * 100
    actual_cov = coverage_stats['combined_coverage_pct']
    coverage_gap = target_cov - actual_cov

    # Format duration
    total_secs = int(elapsed_time)
    hours, rem = divmod(total_secs, 3600)
    mins, secs = divmod(rem, 60)

    if hours:
        duration_label = 'Duration (HH:MM:SS)'
        duration_value = f'{hours:02d}:{mins:02d}:{secs:02d}'
    else:
        duration_label = 'Duration (MM:SS)'
        duration_value = f'{mins:02d}:{secs:02d}'

    # Universe utilization
    uni_size = getattr(metrics, 'total_universe_size', 0)
    universe_util = (metrics.toolsets_sampled / uni_size * 100) if uni_size else 0.0

    # Efficiency rating
    if success_rate >= 50:
        rating = 'Excellent'
    elif success_rate >= 30:
        rating = 'Good'
    elif success_rate >= 15:
        rating = 'Fair'
    else:
        rating = 'Poor'

    stats = {
        'success_rate': success_rate,
        'failure_rate': failure_rate,
        'paths_per_second': paths_per_second,
        'attempts_per_second': attempts_per_second,
        'attempts_per_path': attempts_per_path,
        'efficiency_rating': rating,
        'universe_utilization': universe_util,
        'coverage_gap': coverage_gap,
    }

    if self.silent:
        return stats

    # Print section
    print()
    print('----------------------------------------')
    print('RANDOM SAMPLING STATISTICS')
    print('----------------------------------------')
    print(' - SAMPLING PERFORMANCE:')
    print(f'   Total Attempts:      {total_attempts:,}')
    print(f'   Paths Found:         {total_paths:,}')
    print(f'   Paths Not Found:     {failed:,}')
    print(f'   Success Rate:        {success_rate:.2f}%')
    print(f'   Failure Rate:        {failure_rate:.2f}%')
    print()
    print(' - TIMING INFORMATION:')
    print(f'   Total Duration:      {elapsed_time:.2f} seconds')
    print(f'   {duration_label}:    {duration_value}')
    print()
    print(' - COVERAGE ACHIEVED:')
    print(f'   Target Coverage:     {target_cov:.2f}%')
    print(f'   Actual Coverage:     {actual_cov:.2f}%')
    print(f'   Node Coverage:       {coverage_stats['node_coverage_pct']:.2f}%')
    print(f'   Link Coverage:       {coverage_stats['link_coverage_pct']:.2f}%')
    print(f'   Coverage Gap:        {coverage_gap:.2f}% below target')
    print()
    print(' - EFFICIENCY ANALYSIS:')
    print(f'   Attempts per Path:   {attempts_per_path:.2f}')
    print(f'   Efficiency Rating:   {rating}')
    print(f'   Universe Utilization: {universe_util:.2f}%')
    print()
    print(' - RECOMMENDATIONS:')

    recs: list[str] = []
    if coverage_gap > 10:
        recs.append('Significant coverage gap - may need more sampling or constraint adjustment')
    elif coverage_gap > 5:
        recs.append('Moderate coverage gap - consider extending sampling duration')
    if success_rate < 15:
        recs.append('Low success rate - consider relaxing path constraints')
    if attempts_per_path > 10:
        recs.append('High attempts per path - network connectivity may be sparse')
    if universe_util < 20:
        recs.append('Low universe utilization - consider broader sampling strategy')
    if not recs:
        recs.append('Sampling performance is within acceptable parameters')

    for r in recs:
        print(f'     {r}')

    return stats
  ```
