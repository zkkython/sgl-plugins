import gzip
import json
from typing import Any, Dict, List
import pandas as pd


def parse_trace_hierarchy(events) -> List[Dict[str, Any]]:
    # Sort by start time
    events.sort(key=lambda x: x.get("ts", 0))

    # Create event dictionary for easy lookup by name
    events_by_name = {}
    for event in events:
        name = event.get("name", "")
        if name not in events_by_name:
            events_by_name[name] = []
        events_by_name[name].append(event)

    # Build hierarchy relationships
    hierarchy = []

    # Find root nodes (outermost events)
    root_events = []
    for event in events:
        name = event.get("name", "")
        ts = event.get("ts", 0)
        dur = event.get("dur", 0)
        end_time = ts + dur

        # Check if there is a parent event containing this event
        is_root = True
        for other in events:
            if other == event:
                continue
            other_ts = other.get("ts", 0)
            other_dur = other.get("dur", 0)
            other_end = other_ts + other_dur

            # If other completely contains current event
            if other_ts <= ts and other_end >= end_time:
                # Allow small boundary differences
                if other_ts < ts or other_end > end_time:
                    is_root = False
                    break

        if is_root:
            # Exclude numeric layers (they are sub-layers, not roots)
            if not name.isdigit():
                root_events.append(event)

    # If no obvious root node found, use the earliest event as root
    if not root_events:
        root_events = [min(events, key=lambda x: x.get("ts", 0))]

    # Recursively build hierarchy
    def build_hierarchy(node, depth=0, parent_end_time=None):
        """Recursively build hierarchy structure"""
        node_name = node.get("name", "")
        node_ts = node.get("ts", 0)
        node_dur = node.get("dur", 0)
        node_end = node_ts + node_dur

        # Add current node
        hierarchy.append(
            {
                "name": node_name,
                "depth": depth,
                "start_time": node_ts,
                "duration": node_dur,
                "end_time": node_end,
            }
        )

        # Find child nodes
        children = []
        for event in events:
            if event == node:
                continue

            child_ts = event.get("ts", 0)
            child_dur = event.get("dur", 0)
            child_end = child_ts + child_dur

            # Check if it is a child node of the current node
            # Child node's start time is after current node starts, end time is before current node ends
            if (
                child_ts >= node_ts
                and child_end <= node_end
                and (child_ts > node_ts or child_end < node_end)
            ):  # Ensure not completely identical

                # Check if it is already a child of another node
                is_child_of_other = False
                for other_child in children:
                    other_ts = other_child.get("ts", 0)
                    other_dur = other_child.get("dur", 0)
                    other_end = other_ts + other_dur

                    if other_ts <= child_ts and other_end >= child_end:
                        is_child_of_other = True
                        break

                if not is_child_of_other:
                    # Check if this child event should belong to current node
                    # Determine by name pattern
                    child_depth = depth + 1

                    # If current node is numeric (layer number), its children should be concrete operations
                    if node_name.isdigit():
                        children.append(event)
                    elif child_ts > node_ts and child_end < node_end:
                        # Ensure not contained by other already-added child nodes
                        contained_by_existing_child = False
                        for existing_child in children:
                            existing_ts = existing_child.get("ts", 0)
                            existing_dur = existing_child.get("dur", 0)
                            existing_end = existing_ts + existing_dur
                            if existing_ts <= child_ts and existing_end >= child_end:
                                contained_by_existing_child = True
                                break

                        if not contained_by_existing_child:
                            children.append(event)

        # Sort child nodes by start time
        children.sort(key=lambda x: x.get("ts", 0))

        # Recursively process child nodes
        for child in children:
            build_hierarchy(child, depth + 1, node_end)

    # Start building from root nodes
    for root in sorted(root_events, key=lambda x: x.get("ts", 0)):
        build_hierarchy(root)

    return hierarchy


def parse_trace_events(trace_data) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse hierarchy structure from trace data

    Parameters:
    trace_data: dictionary containing trace events
    """
    events = trace_data.get("traceEvents", [])

    # Filter complete events with type 'cat'
    events = [e for e in events if e.get("cat") == "user_nvtx_annotation"]

    # events to map by key: e.get("pid")
    events_by_pid = {}
    for event in events:
        pid = event.get("pid", 0)
        if pid not in events_by_pid:
            events_by_pid[pid] = []
        events_by_pid[pid].append(event)

    traces_results = {}

    for key, events in events_by_pid.items():
        traces_results[key] = parse_trace_hierarchy(events)

    return traces_results


def save_hierarchy_to_excel(
    hierarchy_map: Dict[str, List[Dict[str, Any]]], filename="model_hierarchy.xlsx"
):
    """
    Save multiple hierarchy structures to Excel file with different sheets

    Parameters:
    hierarchys: list of hierarchy structure lists
    filename: output filename
    """
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        for name, hierarchys in hierarchy_map.items():

            # Prepare data for current hierarchy
            data = []
            for item in hierarchys:
                depth = item["depth"]
                indent = "  " * depth * 5  # Use indentation to represent hierarchy

                # Format time (convert from microseconds to milliseconds)
                start_ms = item["start_time"] / 1000
                duration_ms = item["duration"] / 1000
                end_ms = item["end_time"] / 1000

                data.append(
                    {
                        "Hierarchy": f"{indent}[{item['name']}]",
                        "Start Time (ms)": f"{start_ms:.2f}",
                        "Duration (ms)": f"{duration_ms:.2f}",
                        "End Time (ms)": f"{end_ms:.2f}",
                        "Depth": depth,
                    }
                )

            # Create DataFrame
            df = pd.DataFrame(data)
            sheet_name = (
                f"PID_{name}".replace("[", "")
                .replace("]", "")
                .replace(":", "")
                .replace("/", "")
                .replace("?", "")
                .replace("*", "")
            )
            # Write to Excel
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Get worksheet and adjust column width
            worksheet = writer.sheets[sheet_name]
            worksheet.column_dimensions["A"].width = 80
            worksheet.column_dimensions["B"].width = 15
            worksheet.column_dimensions["C"].width = 15
            worksheet.column_dimensions["D"].width = 15
            worksheet.column_dimensions["E"].width = 10

    print(f"Hierarchy structures have been saved to {filename}")


def analyze_structure(trace_data):
    """
    Analyze model structure and output statistics
    """
    events = trace_data.get("traceEvents", [])
    events = [e for e in events if e.get("cat") == "user_nvtx_annotation"]

    print("=" * 60)
    print("Model Inference Hierarchy Structure Analysis")
    print("=" * 60)

    # Count events by different names
    name_counts = {}
    for event in events:
        name = event.get("name", "")
        name_counts[name] = name_counts.get(name, 0) + 1

    print("\nEvent Statistics:")
    for name, count in sorted(name_counts.items()):
        print(f"  {name}: {count} times")

    # Find layers
    layers = [name for name in name_counts.keys() if name.isdigit()]
    print(f"\nDetected Transformer Layers: {len(layers)} layers")
    print(f"Layer Numbers: {sorted([int(l) for l in layers])}")

    return name_counts


def build_analysis_report(trace_json, filename="transformer_hierarchy.xlsx"):
    print("Analyzing model inference hierarchy structure...")

    # Analyze structure
    analyze_structure(trace_json)

    # Parse hierarchy
    print("\nBuilding hierarchy relationships...")
    hierarchy_map: Dict[str, List[Dict[str, Any]]] = parse_trace_events(trace_json)

    # Save to Excel
    save_hierarchy_to_excel(hierarchy_map, filename)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trace-file",
        type=str,
        required=True,
        help="Path to the trace.json or trace.json.gz file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="transformer_hierarchy.xlsx",
        help="Output file name",
    )
    args = parser.parse_args()
    file_name = args.trace_file
    if file_name.endswith(".gz"):
        with gzip.open(file_name, "rt") as f:
            trace_json = json.load(f)
    else:
        with open(file_name, "r") as f:
            trace_json = json.load(f)
    build_analysis_report(trace_json, args.output)


if __name__ == "__main__":

    main()
