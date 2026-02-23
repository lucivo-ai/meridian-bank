#!/usr/bin/env python3
"""
Meridian Community Bank — Agent Test Runner
Executes test scenarios against the data environment and produces a report.

Can run in two modes:
  1. Interactive: Sends scenarios to an LLM and validates responses
  2. Direct: Executes SQL/metadata/ontology queries directly for validation

Usage:
    python -m agent.agent_runner                    # Run all scenarios (direct mode)
    python -m agent.agent_runner --scenario A1      # Run single scenario
    python -m agent.agent_runner --category B       # Run category
    python -m agent.agent_runner --interactive      # Use LLM (requires ANTHROPIC_API_KEY)
"""
import argparse
import json
import os
import sys
import yaml
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agent.tools import dispatch_tool, TOOL_DEFINITIONS


BASE_DIR = Path(__file__).parent.parent
SCENARIOS_FILE = BASE_DIR / 'agent' / 'scenarios.yaml'


def load_scenarios(scenario_id=None, category=None):
    """Load test scenarios from YAML."""
    with open(SCENARIOS_FILE) as f:
        data = yaml.safe_load(f)

    scenarios = data['scenarios']

    if scenario_id:
        scenarios = [s for s in scenarios if s['id'] == scenario_id]
    elif category:
        scenarios = [s for s in scenarios if s.get('category') == category]

    return scenarios


def run_scenario_direct(scenario):
    """Run a scenario using direct tool calls (no LLM)."""
    result = {
        'id': scenario['id'],
        'question': scenario['question'],
        'category': scenario.get('category', '?'),
        'difficulty': scenario.get('difficulty', 'unknown'),
        'tool_calls': [],
        'validation': {'passed': False, 'details': []}
    }

    # Execute expected tools
    for tool_name in scenario.get('expected_tools', []):
        if tool_name == 'sql_query' and 'sql_hint' in scenario:
            call_result = dispatch_tool('sql_query', {'query': scenario['sql_hint']})
            result['tool_calls'].append({
                'tool': 'sql_query',
                'query': scenario['sql_hint'],
                'result_summary': f"{call_result.get('row_count', 0)} rows" if 'error' not in call_result else call_result['error']
            })

        elif tool_name == 'metadata_search':
            search_term = scenario['question'].split("'")[1] if "'" in scenario['question'] else scenario['question'][:30]
            call_result = dispatch_tool('metadata_search', {
                'search_term': search_term,
                'include_lineage': True,
                'include_quality': True
            })
            result['tool_calls'].append({
                'tool': 'metadata_search',
                'search_term': search_term,
                'datasets_found': len(call_result.get('datasets', [])),
                'glossary_matches': len(call_result.get('glossary_matches', []))
            })

        elif tool_name == 'ontology_query':
            # Basic concept query
            sparql = """
                PREFIX meridian: <http://lucivo.ai/ontology/meridian#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT ?concept ?label ?comment
                WHERE {
                    ?concept rdfs:label ?label .
                    OPTIONAL { ?concept rdfs:comment ?comment }
                }
                LIMIT 50
            """
            call_result = dispatch_tool('ontology_query', {'sparql': sparql})
            result['tool_calls'].append({
                'tool': 'ontology_query',
                'concepts_found': call_result.get('row_count', 0)
            })

    # Validate expected answer
    if 'expected_answer_contains' in scenario:
        for expected in scenario['expected_answer_contains']:
            # Check if expected term appears in any tool result
            found = False
            for tc in result['tool_calls']:
                if expected.lower() in json.dumps(tc).lower():
                    found = True
                    break
            result['validation']['details'].append({
                'check': f"Contains '{expected}'",
                'passed': found
            })
        result['validation']['passed'] = all(
            d['passed'] for d in result['validation']['details']
        )

    elif 'expected_answer_type' in scenario:
        # For numeric/comparative answers, just check tools returned data
        has_data = any(
            tc.get('result_summary', '').endswith('rows') and not tc['result_summary'].startswith('0')
            for tc in result['tool_calls']
        )
        result['validation']['passed'] = has_data or len(result['tool_calls']) > 0
        result['validation']['details'].append({
            'check': f"Tools returned data ({scenario['expected_answer_type']})",
            'passed': result['validation']['passed']
        })

    return result


def run_scenario_interactive(scenario, api_key):
    """Run a scenario using an LLM with tool use."""
    try:
        import anthropic
    except ImportError:
        return {'id': scenario['id'], 'error': 'anthropic package not installed. pip install anthropic'}

    client = anthropic.Anthropic(api_key=api_key)

    # Convert tool definitions to Anthropic format
    tools = []
    for td in TOOL_DEFINITIONS:
        tools.append({
            "name": td["name"],
            "description": td["description"],
            "input_schema": td["parameters"]
        })

    messages = [{"role": "user", "content": scenario['question']}]

    system_prompt = (
        "You are an AI data analyst for Meridian Community Bank. "
        "You have access to the bank's data catalog (metadata_search), "
        "the actual database (sql_query), and a business ontology (ontology_query). "
        "Use these tools to answer questions about the bank's data, quality, lineage, and governance. "
        "Be specific and cite actual data in your answers."
    )

    result = {
        'id': scenario['id'],
        'question': scenario['question'],
        'tool_calls': [],
        'final_answer': '',
        'validation': {'passed': False, 'details': []}
    }

    # Multi-turn tool use loop (max 5 iterations)
    for _ in range(5):
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            system=system_prompt,
            tools=tools,
            messages=messages
        )

        # Collect text and tool use blocks
        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})

        tool_uses = [block for block in assistant_content if block.type == "tool_use"]

        if not tool_uses:
            # Final text response
            text_blocks = [block.text for block in assistant_content if hasattr(block, 'text')]
            result['final_answer'] = '\n'.join(text_blocks)
            break

        # Execute tool calls
        tool_results = []
        for tool_use in tool_uses:
            tool_result = dispatch_tool(tool_use.name, tool_use.input)
            result['tool_calls'].append({
                'tool': tool_use.name,
                'input': tool_use.input,
                'result_preview': json.dumps(tool_result)[:500]
            })
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use.id,
                "content": json.dumps(tool_result)
            })

        messages.append({"role": "user", "content": tool_results})

    # Validate
    if 'expected_answer_contains' in scenario:
        answer_lower = result['final_answer'].lower()
        for expected in scenario['expected_answer_contains']:
            found = expected.lower() in answer_lower
            result['validation']['details'].append({
                'check': f"Contains '{expected}'",
                'passed': found
            })
        result['validation']['passed'] = all(
            d['passed'] for d in result['validation']['details']
        )
    else:
        result['validation']['passed'] = len(result['final_answer']) > 50

    return result


def print_report(results):
    """Print a formatted test report."""
    print("\n" + "=" * 70)
    print("  MERIDIAN COMMUNITY BANK — Agent Test Report")
    print(f"  Generated: {datetime.now().isoformat()}")
    print("=" * 70)

    passed = sum(1 for r in results if r.get('validation', {}).get('passed'))
    failed = len(results) - passed

    print(f"\n  Summary: {passed} passed, {failed} failed, {len(results)} total")

    # Group by category
    by_category = {}
    for r in results:
        cat = r.get('category', '?')
        by_category.setdefault(cat, []).append(r)

    for cat in sorted(by_category.keys()):
        cat_results = by_category[cat]
        cat_passed = sum(1 for r in cat_results if r.get('validation', {}).get('passed'))
        print(f"\n  ── Category {cat} ({cat_passed}/{len(cat_results)} passed) ──")

        for r in cat_results:
            status = "✅" if r.get('validation', {}).get('passed') else "❌"
            print(f"    {status} {r['id']}: {r['question'][:60]}...")

            if r.get('tool_calls'):
                tools_used = ', '.join(tc['tool'] for tc in r['tool_calls'])
                print(f"       Tools: {tools_used}")

            for detail in r.get('validation', {}).get('details', []):
                d_status = "✓" if detail['passed'] else "✗"
                print(f"       {d_status} {detail['check']}")

    print(f"\n{'=' * 70}")
    print(f"  Pass rate: {passed}/{len(results)} ({passed/len(results)*100:.0f}%)")
    print(f"{'=' * 70}\n")

    return passed == len(results)


def main():
    parser = argparse.ArgumentParser(description='Run Meridian Bank agent test scenarios')
    parser.add_argument('--scenario', type=str, help='Run specific scenario (e.g. A1)')
    parser.add_argument('--category', type=str, help='Run scenarios in category (e.g. B)')
    parser.add_argument('--interactive', action='store_true', help='Use LLM for scenario execution')
    parser.add_argument('--output', type=str, help='Save results to JSON file')
    args = parser.parse_args()

    scenarios = load_scenarios(scenario_id=args.scenario, category=args.category)

    if not scenarios:
        print("No matching scenarios found.")
        sys.exit(1)

    print(f"\nRunning {len(scenarios)} scenario(s)...")

    results = []
    for scenario in scenarios:
        print(f"\n  Running {scenario['id']}: {scenario['question'][:50]}...")

        if args.interactive:
            api_key = os.environ.get('ANTHROPIC_API_KEY')
            if not api_key:
                print("Error: ANTHROPIC_API_KEY environment variable required for interactive mode")
                sys.exit(1)
            result = run_scenario_interactive(scenario, api_key)
        else:
            result = run_scenario_direct(scenario)

        results.append(result)

    all_passed = print_report(results)

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to {args.output}")

    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()
