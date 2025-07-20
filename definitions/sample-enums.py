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