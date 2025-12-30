#!/usr/bin/env python
"""Generate Excel hierarchy report from Model Structure events in trace file."""

import gzip
import json
import sys
import os
from typing import Any, Dict, List

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Error: pandas not installed. Please install with: pip install pandas openpyxl")
    sys.exit(1)


def load_trace_file(trace_file: str) -> Dict[str, Any]:
    """Load trace file (supports both .json and .json.gz formats)."""
    print(f"Loading trace file: {trace_file}")
    
    if trace_file.endswith(".gz"):
        with gzip.open(trace_file, "rt", encoding="utf-8") as f:
            trace_data = json.load(f)
    else:
        with open(trace_file, "r", encoding="utf-8") as f:
            trace_data = json.load(f)
    
    print(f"Loaded {len(trace_data.get('traceEvents', []))} events")
    return trace_data


def extract_model_structure_events(trace_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Extract Model Structure events from the trace file."""
    events = trace_data.get("traceEvents", [])
    
    # Filter events from [Model Structure] processes
    model_structure_events = {}
    for event in events:
        pid = str(event.get("pid", ""))
        
        # Check if this is a Model Structure process
        if "[Model Structure]" in pid and event.get("ph") != "M":
            if pid not in model_structure_events:
                model_structure_events[pid] = []
            model_structure_events[pid].append(event)
    
    return model_structure_events


def build_module_hierarchy(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build hierarchical structure from Model Structure events."""
    # Sort by start time
    events = sorted(events, key=lambda x: x.get("ts", 0))
    
    hierarchy = []
    
    # Find root nodes
    root_events = []
    for event in events:
        ts = event.get("ts", 0)
        dur = event.get("dur", 0)
        end_time = ts + dur
        
        is_root = True
        for other in events:
            if other == event:
                continue
            other_ts = other.get("ts", 0)
            other_dur = other.get("dur", 0)
            other_end = other_ts + other_dur
            
            if other_ts <= ts and other_end >= end_time:
                if other_ts < ts or other_end > end_time:
                    is_root = False
                    break
        
        if is_root:
            root_events.append(event)
    
    if not root_events:
        root_events = [min(events, key=lambda x: x.get("ts", 0))]
    
    def build_hierarchy_recursive(node, depth=0):
        node_name = node.get("name", "")
        node_ts = node.get("ts", 0)
        node_dur = node.get("dur", 0)
        node_end = node_ts + node_dur
        
        hierarchy.append({
            "name": node_name,
            "depth": depth,
            "start_time": node_ts,
            "duration": node_dur,
            "end_time": node_end,
        })
        
        children = []
        for event in events:
            if event == node:
                continue
            
            child_ts = event.get("ts", 0)
            child_dur = event.get("dur", 0)
            child_end = child_ts + child_dur
            
            if (child_ts >= node_ts and child_end <= node_end and
                (child_ts > node_ts or child_end < node_end)):
                
                is_child_of_other = False
                for other_child in children:
                    other_ts = other_child.get("ts", 0)
                    other_dur = other_child.get("dur", 0)
                    other_end = other_ts + other_dur
                    
                    if other_ts <= child_ts and other_end >= child_end:
                        is_child_of_other = True
                        break
                
                if not is_child_of_other:
                    children.append(event)
        
        children.sort(key=lambda x: x.get("ts", 0))
        
        for child in children:
            build_hierarchy_recursive(child, depth + 1)
    
    for root in sorted(root_events, key=lambda x: x.get("ts", 0)):
        build_hierarchy_recursive(root)
    
    return hierarchy


def save_model_structure_to_excel(
    hierarchy_map: Dict[str, List[Dict[str, Any]]], 
    filename: str
) -> None:
    """Save Model Structure hierarchy to Excel file."""
    try:
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            for pid_name, hierarchys in hierarchy_map.items():
                data = []
                for item in hierarchys:
                    depth = item["depth"]
                    indent = "  " * depth * 2
                    
                    start_ms = item["start_time"] / 1000
                    duration_ms = item["duration"] / 1000
                    end_ms = item["end_time"] / 1000
                    
                    data.append({
                        "Hierarchy": f"{indent}[{item['name']}]",
                        "Start Time (ms)": f"{start_ms:.2f}",
                        "Duration (ms)": f"{duration_ms:.2f}",
                        "End Time (ms)": f"{end_ms:.2f}",
                        "Depth": depth,
                    })
                
                df = pd.DataFrame(data)
                
                sheet_name = pid_name.replace("[", "").replace("]", "").replace("/", "_")
                sheet_name = sheet_name[:31]
                
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                worksheet = writer.sheets[sheet_name]
                worksheet.column_dimensions["A"].width = 80
                worksheet.column_dimensions["B"].width = 15
                worksheet.column_dimensions["C"].width = 15
                worksheet.column_dimensions["D"].width = 15
                worksheet.column_dimensions["E"].width = 10
        
        print(f"\nModel Structure hierarchy saved to: {filename}")
        
    except Exception as e:
        print(f"Error saving Excel file: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate Excel hierarchy report from nn.Module events in trace file"
    )
    parser.add_argument(
        "trace_file",
        type=str,
        help="Path to trace file (json or json.gz)"
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output Excel filename (default: <trace_file>_model_hierarchy.xlsx)"
    )
    
    args = parser.parse_args()
    
    # Validate input
    if not os.path.exists(args.trace_file):
        print(f"Error: Trace file not found: {args.trace_file}")
        return 1
    
    print(f"Analyzing Model Structure from: {args.trace_file}")
    
    # Load trace file
    trace_data = load_trace_file(args.trace_file)
    
    # Extract Model Structure events
    model_structure_events = extract_model_structure_events(trace_data)
    
    if not model_structure_events:
        print("\nNo Model Structure events found in trace file!")
        print("Make sure the trace file contains [Model Structure] process.")
        print("\nTo generate it, run:")
        print(f"  python profile_kernel.py --trace-file <original.trace.json.gz>")
        return 1
    
    print(f"Found {len(model_structure_events)} Model Structure process(es)")
    
    # Build hierarchy for each process
    hierarchy_map = {}
    for pid, events in model_structure_events.items():
        print(f"Building hierarchy for {pid} ({len(events)} events)...")
        hierarchy = build_module_hierarchy(events)
        hierarchy_map[pid] = hierarchy
    
    # Determine output filename
    if args.output is None:
        base_name = os.path.splitext(args.trace_file)[0]
        if base_name.endswith(".json"):
            base_name = os.path.splitext(base_name)[0]
        args.output = f"{base_name}_model_hierarchy.xlsx"
    
    # Save to Excel
    save_model_structure_to_excel(hierarchy_map, args.output)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
