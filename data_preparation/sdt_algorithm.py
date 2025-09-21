import numpy as np

def sdt_compress(data_points, tolerance_cd):
    if not data_points:
        return []

    compressed_points = []
    
    sorted_points = sorted(data_points, key=lambda p: p['timestamp'])
    
    anchor_idx = 0
    compressed_points.append(sorted_points[anchor_idx])

    while anchor_idx < len(sorted_points) - 1:
        anchor_ts = sorted_points[anchor_idx]['timestamp']
        anchor_value = sorted_points[anchor_idx]['value']

        upper_bound_slopes = []
        lower_bound_slopes = []

        next_point_idx = anchor_idx + 1
        segment_end_idx = anchor_idx

        while next_point_idx < len(sorted_points):
            current_ts = sorted_points[next_point_idx]['timestamp']
            current_value = sorted_points[next_point_idx]['value']

            if current_ts == anchor_ts:
                next_point_idx += 1
                continue

            upper_slope_current = (current_value + tolerance_cd - anchor_value) / (current_ts - anchor_ts)
            lower_slope_current = (current_value - tolerance_cd - anchor_value) / (current_ts - anchor_ts)

            current_upper_slope = min(upper_bound_slopes + [upper_slope_current]) if upper_bound_slopes else upper_slope_current
            current_lower_slope = max(lower_bound_slopes + [lower_slope_current]) if lower_bound_slopes else lower_slope_current
            
            if current_lower_slope > current_upper_slope:
                break

            upper_bound_slopes.append(upper_slope_current)
            lower_bound_slopes.append(lower_slope_current)
            segment_end_idx = next_point_idx
            next_point_idx += 1
        
        if segment_end_idx > anchor_idx:
            if sorted_points[segment_end_idx] not in compressed_points:
                 compressed_points.append(sorted_points[segment_end_idx])
            anchor_idx = segment_end_idx
        else:
            anchor_idx += 1
            if anchor_idx < len(sorted_points) and sorted_points[anchor_idx] not in compressed_points:
                compressed_points.append(sorted_points[anchor_idx])

    if sorted_points and sorted_points[-1] not in compressed_points:
        compressed_points.append(sorted_points[-1])

    compressed_points = sorted(compressed_points, key=lambda p: p['timestamp'])

    return compressed_points