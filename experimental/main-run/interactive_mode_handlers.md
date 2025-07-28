Inconsistency in how CLI arguments are handled in interactive mode. The code always prompts for approach selection regardless of whether `--approach RANDOM` was already provided via CLI.

We need dedicated handler methods similar to `handle_interactive_verbose_choice`. 

```python
def handle_interactive_approach_choice(args) -> Approach:
    '''Handle approach selection in interactive mode, respecting CLI arguments.'''
    if hasattr(args, 'approach') and args.approach:
        print(f'-> Using approach from CLI: {args.approach.value}')
        return args.approach
    
    approach_str = fetch_selected_choice(
        'Select analysis approach:',
        ['RANDOM', 'SCENARIO'],
        default=1,
        required=True,
        description='  - RANDOM: Random sampling\n  - SCENARIO: Scenario selection'
    )
    return Approach(approach_str)


def handle_interactive_method_choice(args, approach: Approach) -> Method:
    '''Handle method selection in interactive mode, respecting CLI arguments.'''
    if hasattr(args, 'method') and args.method:
        print(f'-> Using method from CLI: {args.method.value}')
        return args.method
    
    if approach == Approach.RANDOM:
        method_choices = ['SIMPLE', 'STRATIFIED']
        method_help = '  - SIMPLE: Basic random sampling\n  - STRATIFIED: Stratified random sampling (advanced)'
        
        method_str = fetch_selected_choice(
            f'\n-> Select method for {approach.value} approach:',
            method_choices,
            default=1,
            description=method_help,
        )
        return Method(method_str.upper())
    else:
        method_choices = ['PREDEFINED', 'SYNTHETIC', 'FILE']
        method_help = '  - PREDEFINED: Use existing scenarios\n  - SYNTHETIC: Generate synthetic scenarios\n  - FILE: Use existing path scenarios in a file'
        
        method_str = fetch_selected_choice(
            f'\n-> Select method for {approach.value} approach:',
            method_choices,
            default=1,
            description=method_help,
        )
        # Note: For scenarios, method will be auto-detected from code
        return Method.PREDEFINED  # Will be overridden if scenario code provided


def handle_interactive_coverage_target_choice(args, approach: Approach) -> float:
    '''Handle coverage target selection in interactive mode, respecting CLI arguments.'''
    if approach == Approach.SCENARIO:
        print('\n-> SCENARIO approach uses predefined coverage from scenarios - no target needed.')
        return 0.0
    
    if hasattr(args, 'coverage_target') and args.coverage_target is not None:
        print(f'-> Using coverage target from CLI: {args.coverage_target}')
        return args.coverage_target
    
    return fetch_float_input(
        '\n-> Enter coverage target (as decimal, e.g., 0.15 for 15%)',
        default=0.02,
        min_val=0.001,
        max_val=1.0
    )


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
        # 1. Select approach (respecting CLI arguments)
        approach = handle_interactive_approach_choice(args)
        
        # 2. Select method based on approach (respecting CLI arguments)
        method = handle_interactive_method_choice(args, approach)
        
        # 3. Get coverage target (only for RANDOM approach, respecting CLI arguments)
        coverage_target = handle_interactive_coverage_target_choice(args, approach)
        
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
        print('\n-> Starting analysis run...')
        print('This may take several minutes depending on coverage target and network size.')
        
        result = execute_run_with_config(
            config,
            verbose=verbose
        )
        
        # Print results
        print_run_summary(result, verbose=verbose)
        
        # Exit with appropriate code
        if result.status == RunStatus.DONE:
            print('\n   Analysis completed successfully!')
            print(f'   Results stored with run ID: {result.run_id}')
            sys.exit(0)
        else:
            print('\n  x Analysis failed. Check logs for details.')
            sys.exit(1)
    except Exception as e:
        print(f'\n   x Error: {e}', file=sys.stderr)
        sys.exit(1)
```

The key improvements in this optimized version:

**CLI Argument Priority**: CLI arguments take precedence over interactive prompts. If a user runs `--interactive --approach RANDOM`, the approach selection will be skipped and "RANDOM" will be used directly.

**Consistent Handler Pattern**: Three new handler methods follow the same pattern as `handle_interactive_verbose_choice`:
- `handle_interactive_approach_choice()` - handles approach selection
- `handle_interactive_method_choice()` - handles method selection  
- `handle_interactive_coverage_target_choice()` - handles coverage target

**Python 3.11 Optimizations**:
- Uses single quotes for all strings
- Ready for lowercase type hints (dict, list, tuple) when you update the imports
- Leverages modern Python features and patterns

**Behavior**:
- **With CLI args**: `python script.py --interactive --approach RANDOM --method SIMPLE --coverage-target 0.1` will use all provided values and only prompt for missing parameters
- **Without CLI args**: `python script.py --interactive` will prompt for all parameters interactively
- **Mixed**: `python script.py --interactive --approach RANDOM` will use RANDOM approach but still prompt for method and coverage target

This approach provides the best user experience by respecting explicitly provided CLI arguments while still allowing interactive input for missing parameters.
