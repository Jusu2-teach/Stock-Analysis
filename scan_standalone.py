import re

files = [
    'src/astock/business_engines/reporters/engine.py',
    'src/astock/business_engines/backtest/engine.py',
    'src/astock/business_engines/evaluators/engine.py',
]

skip_prefixes = [
    'if ', 'elif ', 'else:', 'for ', 'while ', 'try:', 'except', 'finally:',
    'with ', 'def ', 'class ', 'return', 'yield', 'raise', 'import ', 'from ',
    'assert ', 'pass', 'break', 'continue', '@', ')', ']', '}',
    'print(', 'logging.', 'logger.', 'lines.append', 'L.append',
    'state_groups', 'industry_stats', 'lifecycle_dist', 'decision_dist',
    'state_dist', 'lc_groups', 'Path(', 'result[', 'factors_dict[',
    'temp[', 'df[', 'test_df[', 'row[', 'factor_ics[', 'weights[',
    'signals[', 'aggregated_trends[', 'company_info_dict[', 'mf[',
]

for fpath in files:
    print(f'\n===== {fpath} =====')
    with open(fpath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    in_docstring = False
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith('\"\"\"') or stripped.startswith("'''"):
            if stripped.count('\"\"\"') == 1 or stripped.count("'''") == 1:
                in_docstring = not in_docstring
            continue
        if in_docstring:
            continue
        if not stripped or stripped.startswith('#'):
            continue
        
        skip = False
        for pf in skip_prefixes:
            if stripped.startswith(pf):
                skip = True
                break
        if skip:
            continue
        
        # Check: is there an assignment on the line?
        has_assignment = False
        for m in re.finditer(r'=', stripped):
            pos = m.start()
            if pos > 0 and stripped[pos-1] not in ('!', '<', '>', '='):
                if pos + 1 < len(stripped) and stripped[pos+1] == '=':
                    continue
                has_assignment = True
                break
        
        if has_assignment:
            continue
        
        # Now we have a line with no assignment - check if it looks like a standalone expression
        # Pattern: identifier.method(...) or identifier[...] or function_call(...)
        if re.match(r'^[a-zA-Z_][\w]*\.[\w]+\(', stripped):
            print(f'  L{i}: {stripped}')
        elif re.match(r'^(sum|len|max|min|abs|float|int|str|bool)\(', stripped):
            print(f'  L{i}: {stripped}')
        elif re.match(r'^_?[a-zA-Z_][\w]*\(', stripped):
            # function call without assignment - could be standalone
            if not stripped.endswith(':'):
                print(f'  L{i}: {stripped}')
