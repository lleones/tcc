import numpy as np

def linear_interpolate(compressed_points, start_ts, end_ts, interval_seconds):
    if not compressed_points:
        return []

    timestamps = np.array([p['timestamp'] for p in compressed_points])
    values = np.array([p['value'] for p in compressed_points])

    new_timestamps = np.arange(start_ts, end_ts + interval_seconds, interval_seconds)
    
    min_comp_ts = timestamps.min()
    max_comp_ts = timestamps.max()
    
    filtered_new_timestamps = [ts for ts in new_timestamps if min_comp_ts <= ts <= max_comp_ts]
    
    if len(filtered_new_timestamps) < 2:
        if filtered_new_timestamps:
            idx = np.searchsorted(timestamps, filtered_new_timestamps[0])
            interpolated_value = values[min(idx, len(values)-1)]
            return [{'timestamp': filtered_new_timestamps[0], 'value': float(interpolated_value)}]
        return []

    interpolated_values = np.interp(filtered_new_timestamps, timestamps, values)

    reconstructed_series = [
        {'timestamp': int(ts), 'value': float(val)}
        for ts, val in zip(filtered_new_timestamps, interpolated_values)
    ]
    return reconstructed_series


def calculate_weighted_average(compressed_points):
    if not compressed_points or len(compressed_points) < 2:
        if compressed_points:
            return compressed_points[0]['value']
        return 0.0

    total_value_time = 0.0
    total_time = 0.0

    for i in range(len(compressed_points) - 1):
        p1 = compressed_points[i]
        p2 = compressed_points[i+1]

        duration = p2['timestamp'] - p1['timestamp']
        average_segment_value = (p1['value'] + p2['value']) / 2.0

        total_value_time += average_segment_value * duration
        total_time += duration

    if total_time == 0:
        return 0.0

    return total_value_time / total_time