@dataclass
class BiasReduction:
    """Configuration for bias reduction in random sampling."""
    max_attempts_per_toolset: int = 5
    max_attempts_per_equipment: int = 3
    min_distance_between_nodes: int = 10
    utility_diversity_weight: float = 0.3
    category_diversity_weight: float = 0.2
    phase_diversity_weight: float = 0.2
    
    plateau_threshold: int = 50  # attempts without improvement
    min_coverage_improvement: float = 0.01  # minimum improvement to reset plateau
    coverage_timeout_multiplier: float = 2.0  # increase attempts when close to target

@dataclass
class CoverageMetrics:
    """Coverage calculation results."""
    total_nodes_in_scope: int
    total_links_in_scope: int
    covered_nodes: int
    covered_links: int
    node_coverage_pct: float
    link_coverage_pct: float
    overall_coverage_pct: float
    unique_paths_count: int

@dataclass
class CoverageCache:
    scope: 'CoverageScope'
    node_coverage: bitarray
    link_coverage: bitarray
    covered_paths: set['PathFound'] = field(default_factory=set)
    last_updated: 'PathFound' = None
    coverage_history: deque(maxlen=config.coverage_history_size)
    attempts_without_improvement: 0
    best_coverage: 0.0

@dataclass
class CoverageScope:
    """Defines the scope for coverage calculation."""
    fab_no: Optional[int] = None
    phase_no: Optional[int] = None
    model_no: Optional[int] = None
    e2e_group_nos: Optional[list[int]] = None

    total_nodes: int = 0
    total_links: int = 0

    node_id_mapping: dict[int, int] = None  # Maps actual node_id to bitarray index
    link_id_mapping: dict[int, int] = None  # Maps actual link_id to bitarray index

@dataclass
class PathFound:
    """Result of a path finding operation."""
    start_node_id: int
    start_poc_id: int
    start_equipment_id: int
    end_node_id: int
    end_poc_id: int
    end_equipment_id: int
    data: Optional['PathData'] = None
    
@dataclass
class PathData:
    nodes: list[int]
    links: list[int]

    total_cost: float
    total_length_mm: float

    e2e_group_nos: Optional[list[int]]
    data_codes: Optional[list[int]]
    utility_nos: Optional[list[int]]
    references: Optional[list[str]]

@dataclass
class RandomRunSummary:
    total_attempts: int
    total_paths_found: int
    unique_paths: int
    target_coverage: Optional[float] = None
    achieved_coverage: Optional[float] = None
    coverage_efficiency: Optional[float] = None

    total_errors: int = 0
    total_reviews: int = 0
    critical_errors: int = 0
    total_nodes: int = 0
    total_links: int = 0
    avg_path_nodes: Optional[float] = None
    avg_path_links: Optional[float] = None
    avg_path_length: Optional[float] = None
    success_rate: Optional[float] = None

@dataclass
class RunSummary:
    """Aggregated run metrics."""
    run_id: str

    total_attempts: int
    total_paths_found: int
    unique_paths: int

    total_scenario_tests: int = 0
    scenario_success_rate: Optional[float] = None

    total_errors: int = 0
    total_review_flags: int = 0
    critical_errors: int = 0

    target_coverage: Optional[float] = None
    achieved_coverage: Optional[float] = None
    coverage_efficiency: Optional[float] = None

    total_nodes: int = 0
    total_links: int = 0

    avg_path_nodes: Optional[float] = None
    avg_path_links: Optional[float] = None
    avg_path_length: Optional[float] = None

    success_rate: Optional[float] = None
    completion_status: RunStatus = RunStatus.COMPLETED

    execution_time_mm: Optional[float] = None
    started_at: datetime = field(default_factory=datetime.now)
    ended_at: Optional[datetime] = None

    summarized_at: datetime = field(default_factory=datetime.now)