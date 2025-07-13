# managers/random.py

import random
import time
from typing import Optional, Any
from dataclasses import dataclass
from collections import defaultdict

from db import Database
from string_helper import StringHelper
from sample_models import (
    RandomRunConfig, BiasReduction, CoverageScope, PathResult,
    Phase, DataTypeModel
)


@dataclass
class SamplingUniverse:
    """Defines the universe of possible sampling targets."""
    fab_nos: list[int]
    toolsets: list[str]
    phase_nos: list[int]
    model_nos: list[int]
    e2e_group_nos: list[int]
    total_equipments: int
    total_pocs: int


@dataclass
class EquipmentInfo:
    """Equipment information for sampling."""
    equipment_id: int
    toolset: str
    node_id: int
    data_code: int
    category_no: int
    fab_no: int
    phase_no: int
    model_no: int
    e2e_group_no: int


@dataclass
class PocInfo:
    """PoC information for sampling."""
    poc_id: int
    equipment_id: int
    node_id: int
    markers: Optional[str]
    reference: Optional[str]
    utility_no: Optional[int]
    flow: Optional[str]
    is_used: bool
    is_loopback: bool


class RandomManager:
    """Random path generation with bias mitigation strategies."""
    
    def __init__(self, db: Database):
        self.db = db
        self.bias_reduction = BiasReduction()
        self._sampling_cache = {}
    
    def build_sampling_universe(self, config: RandomRunConfig) -> SamplingUniverse:
        """Build the universe of possible sampling targets based on filters."""
        filters = {}
        
        if config.fab_no:
            filters['fab_no'] = ('=', config.fab_no)
        if config.phase_no:
            filters['phase_no'] = ('=', config.phase_no)
        if config.model_no:
            filters['model_no'] = ('=', config.model_no)
        if config.e2e_group_no:
            filters['e2e_group_no'] = ('=', config.e2e_group_no)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        # Get unique values for sampling
        universe_sql = f'''
            SELECT DISTINCT 
                ts.fab_no,
                ts.code as toolset,
                ts.phase_no,
                ts.model_no,
                ts.e2e_group_no
            FROM tb_toolsets ts
            JOIN tb_equipments eq ON eq.toolset = ts.code
            JOIN tb_equipment_pocs poc ON poc.equipment_id = eq.id
            {where_clause}
            AND ts.is_active = 1
            AND eq.is_active = 1
            AND poc.is_used = 1
        '''
        
        rows = self.db.query(universe_sql, params)
        
        fab_nos = sorted(list(set(row[0] for row in rows)))
        toolsets = sorted(list(set(row[1] for row in rows)))
        phase_nos = sorted(list(set(row[2] for row in rows)))
        model_nos = sorted(list(set(row[3] for row in rows)))
        e2e_group_nos = sorted(list(set(row[4] for row in rows)))
        
        # Count totals
        count_sql = f'''
            SELECT 
                COUNT(DISTINCT eq.id) as total_equipments,
                COUNT(DISTINCT poc.id) as total_pocs
            FROM tb_toolsets ts
            JOIN tb_equipments eq ON eq.toolset = ts.code
            JOIN tb_equipment_pocs poc ON poc.equipment_id = eq.id
            {where_clause}
            AND ts.is_active = 1
            AND eq.is_active = 1
            AND poc.is_used = 1
        '''
        
        count_row = self.db.query(count_sql, params)[0]
        
        return SamplingUniverse(
            fab_nos=fab_nos,
            toolsets=toolsets,
            phase_nos=phase_nos,
            model_nos=model_nos,
            e2e_group_nos=e2e_group_nos,
            total_equipments=count_row[0],
            total_pocs=count_row[1]
        )
    
    def fetch_toolset_equipments(self, toolset: str, config: RandomRunConfig) -> list[EquipmentInfo]:
        """Fetch all equipments for a given toolset."""
        cache_key = f'toolset_eq_{toolset}_{config.fab_no}_{config.phase_no}_{config.model_no}'
        
        if cache_key in self._sampling_cache:
            return self._sampling_cache[cache_key]
        
        filters = {'eq.toolset': ('=', toolset)}
        
        if config.fab_no:
            filters['ts.fab_no'] = ('=', config.fab_no)
        if config.phase_no:
            filters['ts.phase_no'] = ('=', config.phase_no)
        if config.model_no:
            filters['ts.model_no'] = ('=', config.model_no)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT 
                eq.id, eq.toolset, eq.node_id, eq.data_code, eq.category_no,
                ts.fab_no, ts.phase_no, ts.model_no, ts.e2e_group_no
            FROM tb_equipments eq
            JOIN tb_toolsets ts ON ts.code = eq.toolset
            {where_clause}
            AND eq.is_active = 1
            AND ts.is_active = 1
        '''
        
        rows = self.db.query(sql, params)
        equipments = [EquipmentInfo(*row) for row in rows]
        
        self._sampling_cache[cache_key] = equipments
        return equipments
    
    def fetch_equipment_pocs(self, equipment_id: int) -> list[PocInfo]:
        """Fetch all PoCs for a given equipment."""
        cache_key = f'eq_pocs_{equipment_id}'
        
        if cache_key in self._sampling_cache:
            return self._sampling_cache[cache_key]
        
        sql = '''
            SELECT 
                id, equipment_id, node_id, markers, reference, 
                utility_no, flow, is_used, is_loopback
            FROM tb_equipment_pocs
            WHERE equipment_id = ? AND is_used = 1
        '''
        
        rows = self.db.query(sql, [equipment_id])
        pocs = [PocInfo(*row) for row in rows]
        
        self._sampling_cache[cache_key] = pocs
        return pocs
    
    def sample_random_poc_pair(self, config: RandomRunConfig, 
                              universe: SamplingUniverse,
                              attempt_tracker: dict[str, int]) -> Optional[tuple[PocInfo, PocInfo]]:
        """Sample a random pair of PoCs with bias mitigation."""
        
        max_attempts = 100
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            
            if config.is_inter_toolset:
                # Sample from different toolsets
                if len(universe.toolsets) < 2:
                    return None
                
                toolset1, toolset2 = random.sample(universe.toolsets, 2)
                poc1 = self._sample_poc_from_toolset(toolset1, config, attempt_tracker)
                poc2 = self._sample_poc_from_toolset(toolset2, config, attempt_tracker)
                
            else:
                # Sample from same toolset
                toolset = random.choice(universe.toolsets)
                equipments = self.fetch_toolset_equipments(toolset, config)
                
                if len(equipments) < 2:
                    continue
                
                # Apply bias reduction for equipment selection
                eq1, eq2 = self._select_equipment_pair_with_bias_reduction(
                    equipments, attempt_tracker
                )
                
                if not eq1 or not eq2:
                    continue
                
                poc1 = self._sample_poc_from_equipment(eq1.equipment_id, attempt_tracker)
                poc2 = self._sample_poc_from_equipment(eq2.equipment_id, attempt_tracker)
            
            if poc1 and poc2 and poc1.poc_id != poc2.poc_id:
                return poc1, poc2
        
        return None
    
    def _sample_poc_from_toolset(self, toolset: str, config: RandomRunConfig,
                                attempt_tracker: dict[str, int]) -> Optional[PocInfo]:
        """Sample a PoC from a specific toolset."""
        equipments = self.fetch_toolset_equipments(toolset, config)
        
        if not equipments:
            return None
        
        # Apply bias reduction for equipment selection
        equipment = self._select_equipment_with_bias_reduction(equipments, attempt_tracker)
        
        if not equipment:
            return None
        
        return self._sample_poc_from_equipment(equipment.equipment_id, attempt_tracker)
    
    def _select_equipment_pair_with_bias_reduction(self, equipments: list[EquipmentInfo],
                                                  attempt_tracker: dict[str, int]) -> tuple[Optional[EquipmentInfo], Optional[EquipmentInfo]]:
        """Select equipment pair with bias reduction strategies."""
        if len(equipments) < 2:
            return None, None
        
        # Filter equipments based on attempt limits
        available_equipments = [
            eq for eq in equipments 
            if attempt_tracker.get(f'eq_{eq.equipment_id}', 0) < self.bias_reduction.max_attempts_per_equipment
        ]
        
        if len(available_equipments) < 2:
            # Reset counters if we're running out of options
            for eq in equipments:
                attempt_tracker[f'eq_{eq.equipment_id}'] = 0
            available_equipments = equipments
        
        # Apply diversity weighting
        weighted_equipments = self._apply_diversity_weighting(available_equipments)
        
        # Sample two different equipments
        eq1 = random.choices(weighted_equipments, k=1)[0]
        remaining = [eq for eq in weighted_equipments if eq.equipment_id != eq1.equipment_id]
        
        if not remaining:
            return None, None
        
        eq2 = random.choice(remaining)
        
        # Update attempt counters
        attempt_tracker[f'eq_{eq1.equipment_id}'] = attempt_tracker.get(f'eq_{eq1.equipment_id}', 0) + 1
        attempt_tracker[f'eq_{eq2.equipment_id}'] = attempt_tracker.get(f'eq_{eq2.equipment_id}', 0) + 1
        
        return eq1, eq2
    
    def _select_equipment_with_bias_reduction(self, equipments: list[EquipmentInfo],
                                            attempt_tracker: dict[str, int]) -> Optional[EquipmentInfo]:
        """Select equipment with bias reduction strategies."""
        if not equipments:
            return None
        
        # Filter equipments based on attempt limits
        available_equipments = [
            eq for eq in equipments 
            if attempt_tracker.get(f'eq_{eq.equipment_id}', 0) < self.bias_reduction.max_attempts_per_equipment
        ]
        
        if not available_equipments:
            # Reset counters if we're running out of options
            for eq in equipments:
                attempt_tracker[f'eq_{eq.equipment_id}'] = 0
            available_equipments = equipments
        
        # Apply diversity weighting
        weighted_equipments = self._apply_diversity_weighting(available_equipments)
        equipment = random.choice(weighted_equipments)
        
        # Update attempt counter
        attempt_tracker[f'eq_{equipment.equipment_id}'] = attempt_tracker.get(f'eq_{equipment.equipment_id}', 0) + 1
        
        return equipment
    
    def _apply_diversity_weighting(self, equipments: list[EquipmentInfo]) -> list[EquipmentInfo]:
        """Apply diversity weighting to equipment selection."""
        if len(equipments) <= 1:
            return equipments
        
        # Group by category and phase for diversity
        category_counts = defaultdict(int)
        phase_counts = defaultdict(int)
        
        for eq in equipments:
            category_counts[eq.category_no] += 1
            phase_counts[eq.phase_no] += 1
        
        # Create weighted list favoring less common categories/phases
        weighted_equipments = []
        
        for eq in equipments:
            weight = 1.0
            
            # Reduce weight for overrepresented categories
            if category_counts[eq.category_no] > 1:
                weight *= (1.0 - self.bias_reduction.utility_diversity_weight)
            
            # Reduce weight for overrepresented phases
            if phase_counts[eq.phase_no] > 1:
                weight *= (1.0 - self.bias_reduction.phase_diversity_weight)
            
            # Add equipment multiple times based on weight
            count = max(1, int(weight * 10))
            weighted_equipments.extend([eq] * count)
        
        return weighted_equipments
    
    def _sample_poc_from_equipment(self, equipment_id: int, 
                                  attempt_tracker: dict[str, int]) -> Optional[PocInfo]:
        """Sample a PoC from a specific equipment."""
        pocs = self.fetch_equipment_pocs(equipment_id)
        
        if not pocs:
            return None
        
        # Filter PoCs based on attempt limits
        available_pocs = [
            poc for poc in pocs 
            if attempt_tracker.get(f'poc_{poc.poc_id}', 0) < self.bias_reduction.max_attempts_per_equipment
        ]
        
        if not available_pocs:
            # Reset counters if we're running out of options
            for poc in pocs:
                attempt_tracker[f'poc_{poc.poc_id}'] = 0
            available_pocs = pocs
        
        poc = random.choice(available_pocs)
        
        # Update attempt counter
        attempt_tracker[f'poc_{poc.poc_id}'] = attempt_tracker.get(f'poc_{poc.poc_id}', 0) + 1
        
        return poc
    
    def validate_poc_connectivity(self, poc1: PocInfo, poc2: PocInfo) -> tuple[bool, list[str]]:
        """Validate that PoCs meet basic connectivity requirements."""
        issues = []
        
        # Check if PoCs are from different equipments
        if poc1.equipment_id == poc2.equipment_id:
            issues.append('PoCs are from the same equipment')
        
        # Check if PoCs are properly configured
        if not poc1.is_used:
            issues.append(f'PoC {poc1.poc_id} is not marked as used')
        
        if not poc2.is_used:
            issues.append(f'PoC {poc2.poc_id} is not marked as used')
        
        # Check for required fields
        if not poc1.markers:
            issues.append(f'PoC {poc1.poc_id} missing markers')
        
        if not poc2.markers:
            issues.append(f'PoC {poc2.poc_id} missing markers')
        
        if not poc1.reference:
            issues.append(f'PoC {poc1.poc_id} missing reference')
        
        if not poc2.reference:
            issues.append(f'PoC {poc2.poc_id} missing reference')
        
        # Check utility consistency
        if poc1.utility_no is None and poc2.utility_no is None:
            issues.append('Both PoCs have no utility specified')
        
        return len(issues) == 0, issues
    
    def reset_sampling_cache(self) -> None:
        """Reset the sampling cache to free memory."""
        self._sampling_cache.clear()
    
    def get_sampling_stats(self, attempt_tracker: dict[str, int]) -> dict[str, Any]:
        """Get statistics about sampling attempts."""
        equipment_attempts = [
            count for key, count in attempt_tracker.items() 
            if key.startswith('eq_')
        ]
        
        poc_attempts = [
            count for key, count in attempt_tracker.items() 
            if key.startswith('poc_')
        ]
        
        return {
            'total_equipment_attempts': sum(equipment_attempts),
            'total_poc_attempts': sum(poc_attempts),
            'avg_equipment_attempts': sum(equipment_attempts) / len(equipment_attempts) if equipment_attempts else 0,
            'avg_poc_attempts': sum(poc_attempts) / len(poc_attempts) if poc_attempts else 0,
            'max_equipment_attempts': max(equipment_attempts) if equipment_attempts else 0,
            'max_poc_attempts': max(poc_attempts) if poc_attempts else 0,
            'unique_equipments_sampled': len(equipment_attempts),
            'unique_pocs_sampled': len(poc_attempts)
        }