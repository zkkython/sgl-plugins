"""Extract kernel events from trace files and generate Perfetto-compatible JSON."""

import gzip
import json
import argparse
import os
from typing import Any, Dict, List


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


def extract_kernel_events(trace_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract events with category 'kernel' from trace data."""
    events = trace_data.get("traceEvents", [])
    
    # Filter kernel events
    kernel_events = []
    for event in events:
        cat = event.get("cat", "")
        # Handle both single category and comma-separated categories
        if isinstance(cat, str):
            categories = [c.strip() for c in cat.split(",")]
            if "kernel" in categories:
                kernel_events.append(event)
    
    print(f"Extracted {len(kernel_events)} kernel events")
    return kernel_events


def extract_nn_module_events(trace_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract events with names starting with 'nn.Module' from trace data."""
    events = trace_data.get("traceEvents", [])
    
    # Filter nn.Module events
    nn_module_events = []
    for event in events:
        name = event.get("name", "")
        # Check if event name starts with nn.Module
        if isinstance(name, str) and name.startswith("nn.Module"):
            nn_module_events.append(event)
    
    print(f"Extracted {len(nn_module_events)} nn.Module events")
    return nn_module_events


def analyze_kernel_events(kernel_events: List[Dict[str, Any]]) -> None:
    """Analyze and print statistics about kernel events."""
    if not kernel_events:
        print("No kernel events found!")
        return
    
    print("\n" + "=" * 60)
    print("Kernel Events Analysis")
    print("=" * 60)
    
    # Count by kernel names
    kernel_names = {}
    for event in kernel_events:
        name = event.get("name", "Unknown")
        kernel_names[name] = kernel_names.get(name, 0) + 1
    
    # Count by PIDs
    pids = {}
    for event in kernel_events:
        pid = event.get("pid", "Unknown")
        pids[pid] = pids.get(pid, 0) + 1
    
    # Count by TIDs
    tids = {}
    for event in kernel_events:
        tid = event.get("tid", "Unknown")
        tids[tid] = tids.get(tid, 0) + 1
    
    print(f"\nTotal Kernel Events: {len(kernel_events)}")
    print(f"\nUnique Kernel Names: {len(kernel_names)}")
    print(f"Unique PIDs: {len(pids)}")
    print(f"Unique TIDs: {len(tids)}")
    
    # Show top 10 most frequent kernels
    print("\nTop 10 Most Frequent Kernels:")
    sorted_kernels = sorted(kernel_names.items(), key=lambda x: x[1], reverse=True)
    for i, (name, count) in enumerate(sorted_kernels[:10], 1):
        print(f"  {i}. {name}: {count} times")
    
    # Show PIDs distribution
    print("\nKernel Events by PID:")
    sorted_pids = sorted(pids.items(), key=lambda x: x[1], reverse=True)
    for pid, count in sorted_pids[:5]:
        print(f"  PID {pid}: {count} events")
    
    # Calculate time range
    timestamps = [event.get("ts", 0) for event in kernel_events if "ts" in event]
    if timestamps:
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        duration_ms = (max_ts - min_ts) / 1000.0
        print(f"\nTime Range:")
        print(f"  Start: {min_ts} μs")
        print(f"  End: {max_ts} μs")
        print(f"  Duration: {duration_ms:.2f} ms")


def analyze_nn_module_events(nn_module_events: List[Dict[str, Any]]) -> None:
    """Analyze and print statistics about nn.Module events."""
    if not nn_module_events:
        print("No nn.Module events found!")
        return
    
    print("\n" + "=" * 60)
    print("nn.Module Events Analysis")
    print("=" * 60)
    
    # Count by module names
    module_names = {}
    for event in nn_module_events:
        name = event.get("name", "Unknown")
        module_names[name] = module_names.get(name, 0) + 1
    
    # Count by PIDs
    pids = {}
    for event in nn_module_events:
        pid = event.get("pid", "Unknown")
        pids[pid] = pids.get(pid, 0) + 1
    
    print(f"\nTotal nn.Module Events: {len(nn_module_events)}")
    print(f"Unique Module Names: {len(module_names)}")
    print(f"Unique PIDs: {len(pids)}")
    
    # Show top 10 most frequent modules
    print("\nTop 10 Most Frequent Modules:")
    sorted_modules = sorted(module_names.items(), key=lambda x: x[1], reverse=True)
    for i, (name, count) in enumerate(sorted_modules[:10], 1):
        # Shorten long names for display
        display_name = name if len(name) <= 80 else name[:77] + "..."
        print(f"  {i}. {display_name}: {count} times")
    
    # Calculate time range and total duration
    timestamps = [event.get("ts", 0) for event in nn_module_events if "ts" in event]
    durations = [event.get("dur", 0) for event in nn_module_events if "dur" in event]
    
    if timestamps:
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        time_range_ms = (max_ts - min_ts) / 1000.0
        total_dur_ms = sum(durations) / 1000.0
        
        print(f"\nTime Range:")
        print(f"  Start: {min_ts} μs")
        print(f"  End: {max_ts} μs")
        print(f"  Duration: {time_range_ms:.2f} ms")
        print(f"  Total Module Time: {total_dur_ms:.2f} ms")


def create_perfetto_trace(
    kernel_events: List[Dict[str, Any]], 
    nn_module_events: List[Dict[str, Any]],
    original_trace: Dict[str, Any]
) -> Dict[str, Any]:
    """Create a Perfetto-compatible trace with kernel and nn.Module events."""
    # Combine kernel and nn.Module events
    combined_events = kernel_events + nn_module_events
    
    perfetto_trace = {
        "traceEvents": combined_events,
    }
    
    # Copy metadata fields from original trace
    metadata_fields = [
        "displayTimeUnit", 
        "otherData", 
        "deviceProperties",
        "distributedInfo"
    ]
    
    for field in metadata_fields:
        if field in original_trace:
            perfetto_trace[field] = original_trace[field]
    
    # Add process_name and process_sort_index events if they exist
    all_events = original_trace.get("traceEvents", [])
    metadata_events = [
        e for e in all_events 
        if e.get("ph") == "M" and e.get("name") in ["process_name", "process_sort_index", "thread_name", "thread_sort_index"]
    ]
    
    if metadata_events:
        perfetto_trace["traceEvents"].extend(metadata_events)
    
    return perfetto_trace


def save_perfetto_trace(trace_data: Dict[str, Any], output_file: str) -> None:
    """Save trace data to a JSON file."""
    print(f"\nSaving Perfetto trace to: {output_file}")
    
    # Determine if we should compress
    if output_file.endswith(".gz"):
        with gzip.open(output_file, "wt", encoding="utf-8") as f:
            json.dump(trace_data, f, indent=2)
    else:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(trace_data, f, indent=2)
    
    print(f"Successfully saved {len(trace_data['traceEvents'])} events")
    print(f"\nYou can now open this file in Perfetto UI:")
    print(f"  https://ui.perfetto.dev/")


def main():
    parser = argparse.ArgumentParser(
        description="Extract kernel and nn.Module events from trace file and generate Perfetto-compatible JSON"
    )
    parser.add_argument(
        "--trace-file",
        type=str,
        required=True,
        help="Path to input trace file (.json or .json.gz)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to output JSON file (default: <input>_kernel_module.json)"
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Compress output file with gzip"
    )
    parser.add_argument(
        "--no-analysis",
        action="store_true",
        help="Skip analysis output"
    )
    parser.add_argument(
        "--kernel-only",
        action="store_true",
        help="Extract only kernel events (exclude nn.Module events)"
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.trace_file):
        print(f"Error: Trace file not found: {args.trace_file}")
        return 1
    
    # Determine output filename
    if args.output is None:
        base_name = os.path.splitext(args.trace_file)[0]
        if base_name.endswith(".json"):
            base_name = os.path.splitext(base_name)[0]
        suffix = "_kernel" if args.kernel_only else "_kernel_module"
        args.output = f"{base_name}{suffix}.json"
        if args.compress:
            args.output += ".gz"
    
    # Load trace file
    trace_data = load_trace_file(args.trace_file)
    
    # Extract kernel events
    kernel_events = extract_kernel_events(trace_data)
    
    if not kernel_events:
        print("Warning: No kernel events found in trace file!")
    
    # Extract nn.Module events (unless kernel-only mode)
    nn_module_events = []
    if not args.kernel_only:
        nn_module_events = extract_nn_module_events(trace_data)
        if not nn_module_events:
            print("Warning: No nn.Module events found in trace file!")
    
    # Check if we have any events to export
    if not kernel_events and not nn_module_events:
        print("Error: No relevant events found in trace file!")
        return 1
    
    # Analyze events
    if not args.no_analysis:
        if kernel_events:
            analyze_kernel_events(kernel_events)
        if nn_module_events:
            analyze_nn_module_events(nn_module_events)
    
    # Create Perfetto-compatible trace
    perfetto_trace = create_perfetto_trace(kernel_events, nn_module_events, trace_data)
    
    # Save to output file
    save_perfetto_trace(perfetto_trace, args.output)
    
    return 0


if __name__ == "__main__":
    exit(main())
