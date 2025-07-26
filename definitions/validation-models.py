@dataclass
class ValidationResult:
    """Result of path validation."""
    passed: bool
    errors: int
    counts_by_type: Counter[ValidationType]
    counts_by_severity: Counter[Severity]

@dataclass
class PathLink:
	path_id: int
	seq: int
	
	link_id: int
	length: float
	
	s_node_id: int
	s_node_data_code: int
	s_node_utility_no: int
	
	e_node_id: int
	e_node_data_code: int
	e_node_utility_no: int

	is_reverse: bool
	
	node_flag: Optional[chr] = None # Not used
	

@dataclass
class PathValidation:
    """Represents a path data to execute a validation."""
    run_id: str
    path_id: int

    execution_id: int

    s_node_id: int
    e_node_id: int

    node_count: int
    link_count: int

    data_codes_scope: Optional[list[int]] = None    
    utilities_scope: Optional[list[int]] = None    
    references_scope: Optional[list[int]] = None

    const: float = 0.0
    length_mm:float = 0.0

    execution_status: PathExecutionStatus = None
    execution_time_s: float = 0.0

    node_ids: set[int] = field(default_factory=set)
    linknode_ids: set[int] = field(default_factory=set)

    network: set[PathLink] = field(default_factory=list)

    validation_passed: bool = True


@dataclass
class ValidationTest:
    """Defines a validation test."""
    code: str
    name: str

    scope: ValidationScope
    severity: ValidationSeverity
    test_type: ValidationTestType

    reason: Optional[str] = None

    is_active: bool
    description: Optional[str] = None


@dataclass
class ValidationError:
    """Represents a validation error."""
    
    run_id: str
    path_execution_id: int
    validation_test_id: int

    severity: Severity
    error_scope: ErrorScope  # Is this needed? Or we can use ValidationScope
    error_type: ErrorType

    object_type: str
    object_id: int
    object_guid: str

    error_message: str
    
    id: Optional[int] = None
    
    # Object-specific fields
    object_fab_no: Optional[int] = None
    object_model_no: Optional[int] = None
    object_data_code: Optional[int] = None
    object_e2e_group_no: Optional[int] = None
    object_markers: Optional[str] = None
    object_utility_no: Optional[int] = None
    object_item_no: Optional[int] = None
    object_type_no: Optional[int] = None

    object_material_no: Optional[int] = None
    object_flow: Optional[str] = None
    object_is_loopback: Optional[bool] = None
    object_cost: Optional[float] = None

    error_data: Optional[str] = None  #Used for additional data as a JSON string

    notes: Optional[str] = None