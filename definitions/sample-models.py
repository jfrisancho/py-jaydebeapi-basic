@dataclass
class BiasReduction:
    """Configuration for bias reduction in random sampling."""
    max_attempts_per_toolset: int = 5
    max_attempts_per_equipment: int = 3
    min_distance_between_nodes: int = 10
    utility_diversity_weight: float = 0.3    
    phase_diversity_weight: float = 0.2

@dataclass
class CoverageScope:
    """Defines the scope for coverage calculation."""
    toolset: Optional[str] = None
    fab_no:: Optional[int] = None
    phase_no: Optional[int] = None
    model_no: Optional[int] = None
    e2e_group_no: Optional[int] = None
    total_nodes: int = 0
    total_links: int = 0
    node_id_mapping: Dict[int, int] = None  # Maps actual node_id to bitarray index
    link_id_mapping: Dict[int, int] = None  # Maps actual link_id to bitarray index

@dataclass
class PathResult:
    """Result of a path finding operation."""
    start_node_id: int
    start_poc_id: int
    start_equipment_id: int
    end_node_id: int
    end_poc_id: int
    end_equipment_id: int
    nodes: List[int]
    links: List[int]
    total_cost: float
    total_length_mm: float
    toolset_nos: List[int]
    data_codes: List[int]
    utility_nos: List[int]
    references: List[str]


class DataTypeModel(Enum):
    """Data type model enumeration"""
    BIM = 'BIM'
    C5D = '5D'

class Phase(Enum):
    """Phase enumeration"""
    BIM_P1 = 'P1'
    BIM_P2 = 'P2'
    C5D_A = 'A'
    C5D_B = 'B'
    
    @classmethod
    def normalize(cls, phase_str: Optional[str]) -> Optional['Phase']:
        """Normalize phase string to standard format."""
        if not phase_str:
            return None
            
        s = phase_str.upper().strip()
        
        # Human readable format
        aliases = {
            'PHASE_1': cls.BIM_P1,
            'BIM_P1': cls.BIM_P1,
            'PHASE1': cls.BIM_P1,
            'P1': cls.BIM_P1,
            '1': cls.BIM_P1,
            'PHASE_2': cls.BIM_P2,
            'BIM_P2': cls.BIM_P2,
            'PHASE2': cls.BIM_P2,
            'P2': cls.BIM_P2,
            '2': cls.BIM_P2,
            # Numeric format
            'C5D_A': cls.C5D_A,
            'C5D_B': cls.C5D_B,
            '5D_A': cls.C5D_A,
            '5D_B': cls.C5D_B,
            'C5DA': cls.C5D_A,
            'C5DB': cls.C5D_B,
            '5DA': cls.C5D_A,
            '5DB': cls.C5D_B,
            'A': cls.C5D_A,
            'B': cls.C5D_B,
        }
        
        if s in aliases:
            return aliases[s]
        
        # Try to create from value directly
        try:
            return cls(s)
        except ValueError:
            return None
    
    @classmethod
    def phases(cls) -> List['Phase']:
        """Get all phases."""
        return [cls.BIM_P1, cls.BIM_P2, cls.C5D_A, cls.C5D_B]
    
    @property
    def model(self) -> Optional[DataTypeModel]:
        """Get phase data type model."""
        phase_models = {
            Phase.BIM_P1: DataTypeModel.BIM,
            Phase.C5D_A: DataTypeModel.C5D,
            Phase.BIM_P2: DataTypeModel.BIM,
            Phase.C5D_B: DataTypeModel.C5D,
        }
        return phase_models.get(self, None)
    
    @property
    def cardinal(self) -> Optional[int]:
        """Get numeric value for phase."""
        phase_numbers = {
            Phase.BIM_P1: 1,
            Phase.C5D_A: 1,
            Phase.BIM_P2: 2,
            Phase.C5D_B: 2,
        }
        return phase_numbers.get(self.name, None)
    
    @property
    def conceptual(self) -> str:
        """Get human readable name."""
        return self.name
    
    @property
    def nominal(self) -> str:
        """Get system nomenclature."""
        return self.value  # Returns P1, P2, A, or B

class RunStatus(Enum):
    """Run completion status"""
    INITIALIZED = 'INITIALIZED'
    SAMPLING_COMPLETED = 'SAMPLING_COMPLETED'
    COMPLETED = 'COMPLETED'
    PARTIAL = 'PARTIAL'
    FAILED = 'FAILED'

@dataclass
class RandomRunSummary:
    total_attempts: int
    total_paths_found: int
    unique_paths: int
    target_coverage: Optional[float] = None
    achieved_coverage: Optional[float] = None
    coverage_efficiency: Optional[float] = None

    total_errors: int = 0
    total_review_flags: int = 0
    critical_errors: int = 0
    total_nodes: int = 0
    total_links: int = 0
    avg_path_nodes: Optional[float] = None
    avg_path_links: Optional[float] = None
    avg_path_length: Optional[float] = None
    success_rate: Optional[float] = None

@dataclass
class ScenarioRunSummary:
    unique_paths: int
    total_scenario_tests: int = 0
    scenario_success_rate: Optional[float] = None

    total_errors: int = 0
    total_review_flags: int = 0
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