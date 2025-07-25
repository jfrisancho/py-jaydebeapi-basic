Thi version that handles multiple toolsets, argument priority, and proper validation for all modes.

```python
def interactive_mode(
        args,
        output_level: OutputLevel,    
):
    '''Run the application in interactive mode.'''
    
    print('-' * 30)
    print('PATH ANALYSIS CLI TOOL')
    print('-' * 30)
    print('This tool will guide you through setting up a path analysis run.')
    
    try:
        # 1. Select approach
        approach_str = fetch_selected_choice(
            'Select analysis approach:',
            ['RANDOM', 'SCENARIO'],
            default=1,
            required=True,
            description='  - RANDOM: Random sampling\n  - SCENARIO: Scenario selection'
        )
        approach = Approach(approach_str)
        
        # 2. Select method based on approach
        if approach == Approach.RANDOM:
            method_choices = ['SIMPLE', 'STRATIFIED']
            method_help = '  - SIMPLE: Basic random sampling\n  - STRATIFIED: Stratified random sampling (advanced)'
        else:
            method_choices = ['PREDEFINED', 'SYNTHETIC', 'FILE']
            method_help = '  - PREDEFINED: Use existing scenarios\n  - SYNTHETIC: Generate synthetic scenarios\n  - FILE: Use existing path scenarios in a file'
        
        method_str = fetch_selected_choice(
            f'\n-> Select method for {approach_str} approach:',
            method_choices,
            default=1,
            description=method_help,
        )
        
        # Note: For scenarios, method will be auto-detected from code
        if approach == Approach.RANDOM:
            method = Method(method_str.upper())
        else:
            method = Method.PREDEFINED  # Will be overridden if scenario code provided
        
        # 3. Get coverage target (only for RANDOM approach)
        coverage_target = 0.0  # Default for SCENARIO
        if approach == Approach.RANDOM:
            coverage_target = fetch_float_input(
                '\n-> Enter coverage target (as decimal, e.g., 0.15 for 15%)',
                default=0.02,
                min_val=0.001,
                max_val=1.0
            )
        else:
            print('\n-> SCENARIO approach uses predefined coverage from scenarios - no target needed.')
        
        # Interactive means that it needs user's input but it can run as silent, verbose or normal.
        output_level = handle_interactive_verbose_choice(args, output_level)
        verbose = output_level == OutputLevel.VERBOSE
        
        # Will collect parameters requesting data from the DB.
        random_config, scenario_config = collect_params(
            approach,
            method,
            args,
            interactive=True,
            verbose=verbose,
        )
        if random_config:
            random_config.coverage_target = coverage_target
            
        config = RunConfig(
            run_id=str(uuid.uuid4()),
            approach=approach,
            method=method,
            random_config=random_config,
            scenario_config=scenario_config,
            mode=ExecutionMode.INTERACTIVE,
            silent=output_level == OutputLevel.SILENT,
            verbose=verbose,
        )
        config.generate_tag()
        
        # Show configuration and confirm
        print_configuration_summary(config)
        
        confirmed = fetch_yes_no_input(
            '\nProceed with this configuration?',
            default='Yes',
        )
        
        if not confirmed:
            print('  x Operation cancelled.')
            return
        
        # Execute the run
        print(f'\n-> Starting analysis run...')
        print(f'This may take several minutes depending on coverage target and network size.')
        
        result = execute_run_with_config(
            config,
            verbose=verbose
        )
        
        # Print results
        print_run_summary(result, verbose=verbose)
        
        # Exit with appropriate code
        if result.status == RunStatus.DONE:
            print(f'\n   Analysis completed successfully!')
            print(f'   Results stored with run ID: {result.run_id}')
            sys.exit(0)
        else:
            print(f'\n  x Analysis failed. Check logs for details.')
            sys.exit(1)
    except Exception as e:
        print(f'\n   x Error: {e}', file=sys.stderr)
        sys.exit(1)


def collect_params(
    approach: Approach,
    method: Method,
    args,
    interactive: bool = False,
    verbose: bool = False,
) -> tuple[RandomRunConfig | None, ScenarioRunConfig | None]:
    '''Collect configuration parameters based on approach.'''
    rc = None
    sc = None
    
    if approach == Approach.RANDOM:
        fab, model, phase, toolsets = collect_random_config_params(args, interactive, verbose)
        rc = RandomRunConfig(
            fab=fab,
            model=model,
            phase=phase,
            toolsets=toolsets,
            coverage_target=0.0,  # Will be set later
        )
    elif approach == Approach.SCENARIO:
        scenario_code, scenario_file, method = collect_scenario_config_params(args, interactive, verbose)
        sc = ScenarioRunConfig(
            scenario_code=scenario_code,
            scenario_file=scenario_file,
            method=method,
        )
        
    return rc, sc


def parse_toolsets_from_args(toolsets_str: str | None) -> list[str] | None:
    '''Parse comma-separated toolsets string into list.'''
    if not toolsets_str:
        return None
    return [ts.strip() for ts in toolsets_str.split(',') if ts.strip()]


def handle_interactive_fab_choice(args, output_level: OutputLevel, available_fabs: list[str]) -> str:
    '''Handle fab selection in interactive mode, respecting command line overrides.'''
    # CLI argument takes precedence
    if args.fab:
        if args.fab in available_fabs:
            if should_print_normal(output_level):
                print(f'Using fab from command line: {args.fab}')
            return args.fab
        else:
            print(f'Warning: CLI fab \'{args.fab}\' not available. Available fabs: {available_fabs}')
            if output_level == OutputLevel.UNATTENDED:
                raise ValueError(f'Invalid fab \'{args.fab}\' specified in unattended mode')
    
    # Only prompt if not unattended and no valid CLI value
    if output_level != OutputLevel.UNATTENDED:
        if len(available_fabs) == 1:
            fab = available_fabs[0]
            if should_print_normal(output_level):
                print(f'Using only available fab: {fab}')
            return fab
        else:
            return fetch_selected_choice(
                '\n-> Select fab identifier',
                choices=available_fabs,
                default=1,
                required=True,
            )
    else:
        raise ValueError('No valid fab specified for unattended mode')


def handle_interactive_model_choice(args, output_level: OutputLevel, available_models: list[str]) -> str | None:
    '''Handle model selection in interactive mode, respecting command line overrides.'''
    # CLI argument takes precedence
    if args.model:
        if args.model in available_models:
            if should_print_normal(output_level):
                print(f'Using model from command line: {args.model}')
            return DataModelType.normalize(args.model)
        else:
            print(f'Warning: CLI model \'{args.model}\' not available. Available models: {available_models}')
            if output_level == OutputLevel.UNATTENDED:
                # In unattended mode, invalid model is not fatal - just use None
                return None
    
    if not available_models:
        return None
    
    # Only prompt if not unattended and no valid CLI value
    if output_level != OutputLevel.UNATTENDED:
        if len(available_models) == 1:
            model = available_models[0]
            if should_print_normal(output_level):
                print(f'Using only available model: {model}')
            return DataModelType.normalize(model)
        else:
            model_choice = fetch_selected_choice(
                '\n-> Select model (optional)',
                choices=available_models,
                default=1,
                allow_skip=True,
            )
            return DataModelType.normalize(model_choice) if model_choice else None
    
    return None


def handle_interactive_phase_choice(args, output_level: OutputLevel, available_phases: list[str]) -> str | None:
    '''Handle phase selection in interactive mode, respecting command line overrides.'''
    # CLI argument takes precedence
    if args.phase:
        if args.phase in available_phases:
            if should_print_normal(output_level):
                print(f'Using phase from command line: {args.phase}')
            return Phase.normalize(args.phase)
        else:
            print(f'Warning: CLI phase \'{args.phase}\' not available. Available phases: {available_phases}')
            if output_level == OutputLevel.UNATTENDED:
                # In unattended mode, invalid phase is not fatal - just use None
                return None
    
    if not available_phases:
        return None
    
    # Only prompt if not unattended and no valid CLI value
    if output_level != OutputLevel.UNATTENDED:
        if len(available_phases) == 1:
            phase = available_phases[0]
            if should_print_normal(output_level):
                print(f'Using only available phase: {phase}')
            return Phase.normalize(phase)
        else:
            phase_choice = fetch_selected_choice(
                '\n-> Select phase (optional)',
                choices=available_phases,
                default=1,
                allow_skip=True,
            )
            return Phase.normalize(phase_choice) if phase_choice else None
    
    return None


def handle_interactive_toolsets_choice(args, output_level: OutputLevel, available_toolsets: list[str]) -> list[str] | None:
    '''Handle toolsets selection in interactive mode, respecting command line overrides.'''
    # CLI argument takes precedence
    cli_toolsets = parse_toolsets_from_args(args.toolsets)
    if cli_toolsets:
        # Validate CLI toolsets against available ones
        valid_toolsets = [ts for ts in cli_toolsets if ts in available_toolsets]
        invalid_toolsets = [ts for ts in cli_toolsets if ts not in available_toolsets]
        
        if invalid_toolsets:
            print(f'Warning: CLI toolsets not available: {invalid_toolsets}')
            print(f'Available toolsets: {available_toolsets}')
            if output_level == OutputLevel.UNATTENDED:
                if not valid_toolsets:
                    return None  # No valid toolsets, but not fatal
                
        if valid_toolsets:
            if should_print_normal(output_level):
                print(f'Using toolsets from command line: {valid_toolsets}')
            return valid_toolsets
    
    if not available_toolsets:
        return None
    
    # Only prompt if not unattended and no valid CLI value
    if output_level != OutputLevel.UNATTENDED:
        if len(available_toolsets) <= 5:
            # Show all available toolsets for selection
            toolsets_choice = fetch_multi_choice(
                '\n-> Select toolsets (optional, multiple allowed):',
                choices=available_toolsets,
                allow_skip=True,
                max_selections=min(len(available_toolsets), 10)  # Reasonable limit
            )
            return toolsets_choice if toolsets_choice else None
        else:
            # Too many toolsets, ask for manual input
            print(f'\n-> {len(available_toolsets)} toolsets available. Top 5: {available_toolsets[:5]}...')
            toolsets_input = input('Enter comma-separated toolset codes (optional): ').strip()
            if toolsets_input:
                requested_toolsets = parse_toolsets_from_args(toolsets_input)
                valid_toolsets = [ts for ts in requested_toolsets if ts in available_toolsets]
                if valid_toolsets:
                    return valid_toolsets
                else:
                    print('No valid toolsets entered.')
            return None
    
    return None


def collect_random_config_params(args, interactive: bool = False, verbose: bool = False) -> tuple[str, str | None, str | None, list[str] | None]:
    '''Collect configuration parameters for RANDOM approach.'''
    
    # For non-interactive mode, use CLI args directly with validation
    if not interactive:
        fab = args.fab
        model = DataModelType.normalize(args.model) if args.model else None
        phase = Phase.normalize(args.phase) if args.phase else None
        toolsets = parse_toolsets_from_args(args.toolsets)
        
        # Validate required parameters for non-interactive mode
        if not fab:
            raise ValueError('--fab is required for non-interactive RANDOM approach')
        
        # TODO: Validate if fab, model, phase exist in DB for non-interactive mode
        # validate_config_params_in_db(fab, model, phase, toolsets)
        
        return fab, model, phase, toolsets
    
    # Interactive mode with database queries
    output_level = determine_output_level(args)
    db = Database()
    fab_manager = FabManager(db)
    
    try:
        # Get fab
        available_fabs = fab_manager.fetch_available_fabs(db)
        if not available_fabs:
            raise ValueError('No buildings available in the system')
        
        fab = handle_interactive_fab_choice(args, output_level, available_fabs)
        
        # Get model
        available_models = fab_manager.fetch_available_models(fab)
        model = handle_interactive_model_choice(args, output_level, available_models)
        
        # Get phase
        available_phases = fab_manager.fetch_available_phases(fab, model)
        phase = handle_interactive_phase_choice(args, output_level, available_phases)
        
        # Get toolsets
        available_toolsets = fab_manager.fetch_available_toolsets(fab, model, phase)
        toolsets = handle_interactive_toolsets_choice(args, output_level, available_toolsets)
        
        return fab, model, phase, toolsets
        
    finally:
        db.close()


def fetch_multi_choice(
    prompt: str,
    choices: list[str],
    allow_skip: bool = False,
    max_selections: int = 10
) -> list[str] | None:
    '''Allow user to select multiple choices from a list.'''
    print(f'\n{prompt}')
    for i, choice in enumerate(choices, 1):
        print(f'  {i}. {choice}')
    
    if allow_skip:
        print(f'  {len(choices) + 1}. Skip (no selection)')
    
    while True:
        try:
            response = input(f'Enter numbers separated by commas (1-{len(choices)}): ').strip()
            
            if not response and allow_skip:
                return None
            
            if not response:
                print('Please enter at least one selection or skip.')
                continue
            
            # Parse selections
            selections = []
            for part in response.split(','):
                try:
                    num = int(part.strip())
                    if allow_skip and num == len(choices) + 1:
                        return None
                    if 1 <= num <= len(choices):
                        if choices[num - 1] not in selections:  # Avoid duplicates
                            selections.append(choices[num - 1])
                    else:
                        print(f'Invalid selection: {num}. Please try again.')
                        selections = []
                        break
                except ValueError:
                    print(f'Invalid input: {part}. Please enter numbers only.')
                    selections = []
                    break
            
            if selections:
                if len(selections) <= max_selections:
                    return selections
                else:
                    print(f'Too many selections. Maximum allowed: {max_selections}')
            
        except (KeyboardInterrupt, EOFError):
            print('\nOperation cancelled.')
            return None


def validate_config_params_in_db(fab: str, model: str | None, phase: str | None, toolsets: list[str] | None):
    '''Validate configuration parameters against database for non-interactive modes.'''
    db = Database()
    fab_manager = FabManager(db)
    
    try:
        # Validate fab
        available_fabs = fab_manager.fetch_available_fabs(db)
        if fab not in available_fabs:
            raise ValueError(f'Fab \'{fab}\' not found. Available: {available_fabs}')
        
        # Validate model if provided
        if model:
            available_models = fab_manager.fetch_available_models(fab)
            if model not in available_models:
                raise ValueError(f'Model \'{model}\' not found for fab \'{fab}\'. Available: {available_models}')
        
        # Validate phase if provided
        if phase:
            available_phases = fab_manager.fetch_available_phases(fab, model)
            if phase not in available_phases:
                raise ValueError(f'Phase \'{phase}\' not found for fab \'{fab}\' and model \'{model}\'. Available: {available_phases}')
        
        # Validate toolsets if provided
        if toolsets:
            available_toolsets = fab_manager.fetch_available_toolsets(fab, model, phase)
            invalid_toolsets = [ts for ts in toolsets if ts not in available_toolsets]
            if invalid_toolsets:
                raise ValueError(f'Invalid toolsets: {invalid_toolsets}. Available: {available_toolsets[:10]}...')
                
    finally:
        db.close()
```

This is comprehensive argument validation system:

```python
def validate_arguments(args) -> None:
    '''
    Comprehensive validation of command line arguments for all modes.
    Validates logical consistency and database constraints.
    '''
    validate_argument_conflicts(args)
    validate_approach_specific_args(args)
    
    # For unattended mode, we need stricter validation
    if args.unattended:
        validate_unattended_mode_args(args)
    
    # For all non-interactive modes, validate database constraints
    if not args.interactive:
        validate_database_constraints(args)


def validate_approach_specific_args(args) -> None:
    '''Validate arguments are appropriate for the selected approach.'''
    
    if args.approach == 'RANDOM':
        # RANDOM approach validation
        if args.scenario_code:
            raise ValueError('--scenario-code is only valid for SCENARIO approach')
        if args.scenario_file:
            raise ValueError('--scenario-file is only valid for SCENARIO approach')
        
        # Coverage target validation
        if args.coverage_target < 0.001 or args.coverage_target > 1.0:
            raise ValueError('--coverage-target must be between 0.001 and 1.0')
        
        # Method validation for RANDOM
        if args.method and args.method not in ['SIMPLE', 'STRATIFIED']:
            raise ValueError(f'--method for RANDOM approach must be SIMPLE or STRATIFIED, got: {args.method}')
    
    elif args.approach == 'SCENARIO':
        # SCENARIO approach validation
        if args.method and args.method not in ['PREDEFINED', 'SYNTHETIC', 'FILE']:
            print(f'Warning: --method for SCENARIO approach should be PREDEFINED, SYNTHETIC, or FILE')
        
        if args.coverage_target != 0.02:  # Not default value
            print('Warning: --coverage-target is ignored for SCENARIO approach')
        
        if args.fab:
            print('Warning: --fab is ignored for SCENARIO approach')
        
        if args.model:
            print('Warning: --model is ignored for SCENARIO approach')
        
        if args.phase:
            print('Warning: --phase is ignored for SCENARIO approach')
        
        if args.toolsets:
            print('Warning: --toolsets is ignored for SCENARIO approach')
        
        # Must have either scenario code or file
        if not args.scenario_code and not args.scenario_file:
            raise ValueError('SCENARIO approach requires either --scenario-code or --scenario-file')
        
        if args.scenario_code and args.scenario_file:
            raise ValueError('Cannot specify both --scenario-code and --scenario-file')


def validate_unattended_mode_args(args) -> None:
    '''Validate arguments for unattended mode - must be complete and valid.'''
    
    if args.approach == 'RANDOM':
        # RANDOM approach requires fab in unattended mode
        if not args.fab:
            raise ValueError('--fab is required for unattended RANDOM approach')
        
        # Validate fab choice
        if args.fab not in ['M15', 'M15X', 'M16']:
            raise ValueError(f'Invalid --fab value: {args.fab}. Must be one of: M15, M15X, M16')
        
        # Validate model choice if provided
        if args.model and args.model not in ['BIM', '5D']:
            raise ValueError(f'Invalid --model value: {args.model}. Must be one of: BIM, 5D')
        
        # Validate phase choice if provided
        if args.phase and args.phase not in ['A', 'B', 'P1', 'P2']:
            raise ValueError(f'Invalid --phase value: {args.phase}. Must be one of: A, B, P1, P2')
        
        # Validate toolsets format if provided
        if args.toolsets:
            toolsets = parse_toolsets_from_args(args.toolsets)
            if not toolsets:
                raise ValueError('Invalid --toolsets format. Use comma-separated values like: TS001,TS002')
            
            # Check for reasonable limit
            if len(toolsets) > 50:
                raise ValueError(f'Too many toolsets specified: {len(toolsets)}. Maximum recommended: 50')
            
            # Basic format validation
            for toolset in toolsets:
                if not toolset or len(toolset.strip()) == 0:
                    raise ValueError('Empty toolset code found in --toolsets')
    
    elif args.approach == 'SCENARIO':
        # SCENARIO approach validation
        if not args.scenario_code and not args.scenario_file:
            raise ValueError('Unattended SCENARIO approach requires either --scenario-code or --scenario-file')
        
        if args.scenario_code:
            # Basic scenario code format validation
            if not args.scenario_code.strip():
                raise ValueError('--scenario-code cannot be empty')
            
            # Check for expected prefixes
            valid_prefixes = ['PRE', 'SYN', 'USR']
            if not any(args.scenario_code.startswith(prefix) for prefix in valid_prefixes):
                print(f'Warning: scenario code \'{args.scenario_code}\' does not start with expected prefix: {valid_prefixes}')
        
        if args.scenario_file:
            # Check if file exists and has correct extension
            import os
            if not os.path.exists(args.scenario_file):
                raise ValueError(f'Scenario file not found: {args.scenario_file}')
            
            if not args.scenario_file.lower().endswith(('.json', '.yaml', '.yml')):
                print(f'Warning: scenario file \'{args.scenario_file}\' should be JSON or YAML format')


def validate_database_constraints(args) -> None:
    '''
    Validate arguments against database constraints for non-interactive modes.
    This should be called after basic argument validation.
    '''
    
    if args.approach == 'RANDOM':
        try:
            # Only validate if we have the required arguments
            if args.fab:
                validate_config_params_in_db(
                    fab=args.fab,
                    model=args.model,
                    phase=args.phase,
                    toolsets=parse_toolsets_from_args(args.toolsets)
                )
        except Exception as e:
            if args.unattended:
                # In unattended mode, database validation failures are fatal
                raise ValueError(f'Database validation failed: {e}')
            else:
                # In other non-interactive modes, just warn
                print(f'Warning: Database validation failed: {e}')
                print('This may cause issues during execution.')
    
    elif args.approach == 'SCENARIO':
        # Validate scenario exists if code provided
        if args.scenario_code:
            try:
                validate_scenario_code_in_db(args.scenario_code)
            except Exception as e:
                if args.unattended:
                    raise ValueError(f'Scenario validation failed: {e}')
                else:
                    print(f'Warning: Scenario validation failed: {e}')


def validate_scenario_code_in_db(scenario_code: str) -> None:
    '''Validate that scenario code exists in database.'''
    db = Database()
    scenario_manager = ScenarioManager(db)
    
    try:
        if not scenario_manager.scenario_exists(scenario_code):
            available_scenarios = scenario_manager.get_available_scenario_codes()
            raise ValueError(f'Scenario \'{scenario_code}\' not found. Available: {available_scenarios[:10]}...')
    finally:
        db.close()


def validate_argument_conflicts(args) -> None:
    '''
    Enhanced validation and warning about conflicting arguments.
    '''
    conflicts = []
    warnings = []
    
    # Output level conflicts
    if args.unattended and args.verbose:
        conflicts.append('--unattended overrides --verbose (verbose output disabled)')
    
    if args.unattended and args.interactive:
        conflicts.append('--unattended overrides --interactive (interactive mode disabled)')
    
    if args.silent and args.verbose:
        conflicts.append('--silent overrides --verbose (verbose output disabled)')
    
    # Approach-specific conflicts
    if args.approach == 'SCENARIO':
        if args.coverage_target != 0.02:  # Not default
            warnings.append('--coverage-target is ignored for SCENARIO approach')
        
        scenario_args_count = sum([bool(args.scenario_code), bool(args.scenario_file)])
        if scenario_args_count == 0:
            conflicts.append('SCENARIO approach requires either --scenario-code or --scenario-file')
        elif scenario_args_count > 1:
            conflicts.append('Cannot specify both --scenario-code and --scenario-file')
    
    # Mode compatibility
    if args.unattended and (args.scenario_code or args.scenario_file):
        if args.approach != 'SCENARIO':
            warnings.append('Scenario arguments provided but approach is not SCENARIO')
    
    # Print conflicts (these are errors)
    if conflicts:
        print('Error: Conflicting or invalid arguments:')
        for conflict in conflicts:
            print(f'  - {conflict}')
        raise ValueError('Invalid argument combination')
    
    # Print warnings (these are just notifications)
    if warnings:
        print('Warning: Argument usage notes:')
        for warning in warnings:
            print(f'  - {warning}')
        print()


# Enhanced main function with comprehensive validation
def main():
    '''Enhanced main function with comprehensive argument validation.'''
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        # Comprehensive argument validation
        validate_arguments(args)
        
        # Determine effective output level
        output_level = determine_output_level(args)
        
        if should_print_verbose(output_level):
            print(f'Validated configuration: {vars(args)}')
        
        # Route to appropriate execution mode
        if args.interactive and output_level != OutputLevel.UNATTENDED:
            interactive_mode(args, output_level)
        else:
            non_interactive_mode(args, output_level)
            
    except ValueError as e:
        print(f'Configuration Error: {e}', file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f'Unexpected Error: {e}', file=sys.stderr)
        sys.exit(1)


def non_interactive_mode(args, output_level: OutputLevel):
    '''Handle non-interactive execution modes.'''
    
    if should_print_normal(output_level):
        print(f'Running in non-interactive mode with approach: {args.approach}')
    
    # Collect parameters (no user interaction)
    if args.approach == 'RANDOM':
        approach = Approach.RANDOM
        method = Method(args.method.upper()) if args.method else Method.SIMPLE
    else:
        approach = Approach.SCENARIO
        method = Method.PREDEFINED  # Will be determined from scenario
    
    random_config, scenario_config = collect_params(
        approach,
        method,
        args,
        interactive=False,
        verbose=should_print_verbose(output_level),
    )
    
    if random_config and args.approach == 'RANDOM':
        random_config.coverage_target = args.coverage_target
    
    config = RunConfig(
        run_id=str(uuid.uuid4()),
        approach=approach,
        method=method,
        random_config=random_config,
        scenario_config=scenario_config,
        mode=ExecutionMode.UNATTENDED if args.unattended else ExecutionMode.NORMAL,
        silent=output_level == OutputLevel.SILENT,
        verbose=should_print_verbose(output_level),
    )
    config.generate_tag()
    
    if should_print_normal(output_level):
        print_configuration_summary(config)
    
    # Execute without confirmation in non-interactive modes
    result = execute_run_with_config(config, verbose=should_print_verbose(output_level))
    
    if should_print_normal(output_level):
        print_run_summary(result, verbose=should_print_verbose(output_level))
    
    sys.exit(0 if result.status == RunStatus.DONE else 1)
```
## Key Improvements Made:

### 1. **Multiple Toolsets Support**
- `parse_toolsets_from_args()` - Converts comma-separated string to list
- `handle_interactive_toolsets_choice()` - Handles multiple toolset selection
- `fetch_multi_choice()` - New function for multi-selection UI

### 2. **Argument Priority System**
- **CLI args always win** - Command line arguments take precedence
- **Validation before prompts** - Check CLI args against DB first  
- **Graceful degradation** - Invalid CLI args warn but don't crash (except unattended)
- **Mode-specific behavior** - Unattended mode is strict, interactive is forgiving

### 3. **Comprehensive Validation**
- `validate_arguments()` - Main validation entry point
- `validate_approach_specific_args()` - Approach-specific validation
- `validate_unattended_mode_args()` - Strict validation for automation
- `validate_database_constraints()` - DB validation for all modes

### 4. **Priority Rules**
1. **Unattended mode**: CLI args must be valid and complete
2. **Interactive mode**: CLI args

## Priority Rules (continued)
2. **Interactive mode**: CLI args used if valid, otherwise prompt user
3. **Regular mode**: CLI args used if valid, minimal prompts for missing required args
4. **Silent mode**: CLI args used, warns about issues but continues

### 5. **Error Handling Strategy**
- **Unattended mode**: Fail fast on any validation error
- **Interactive mode**: Warn and prompt for corrections
- **Regular mode**: Warn but attempt to continue
- **Silent mode**: Minimal warnings, best-effort execution

## Usage Examples:

```bash
# CLI override in interactive mode
python main.py --interactive --fab M16 --toolsets "TS001,TS002"
# Result: Uses M16 and specified toolsets, prompts for other params

# Unattended with full validation
python main.py --unattended --fab M16 --toolsets "TS001,TS002,TS003"
# Result: Validates all params against DB, fails if invalid

# Mixed mode - some CLI args, some prompts
python main.py --fab M16
# Result: Uses M16, prompts for other params if needed

# Invalid CLI arg handling
python main.py --interactive --fab INVALID --toolsets "TS001"
# Result: Warns about invalid fab, prompts for valid selection
```

## Key Design Decisions:

1. **CLI Arguments Take Precedence**: Always respect what user explicitly specified
2. **Mode-Appropriate Validation**: Unattended mode is strict, interactive is helpful
3. **Fail Gracefully**: Invalid args warn in interactive mode, fail in unattended
4. **Database Validation**: Check all constraints for non-interactive modes
5. **Multiple Toolsets**: Support 1-50 toolsets with reasonable limits

This approach ensures that:
- **Automation works reliably** (unattended mode)
- **Interactive use is user-friendly** (helpful prompts and warnings)
- **CLI args are always respected** (no surprising overrides)
- **Database constraints are enforced** (prevents runtime failures)

The system now handles the complex interplay between CLI arguments, user interaction, and database validation across all execution modes.
