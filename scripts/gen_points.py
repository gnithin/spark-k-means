#!/usr/bin/env python3
import sys

'''
Script to generate a set of points for the given k.

The output will be a list of points 
For example - 
$ ./gen_points.py 2
0,0
-1,0
1,0
0,1
0,-1
1,1
-1,-1
1,-1
-1,1
104,104
103,104
105,104
104,105
104,103
105,105
103,103
105,103
103,105

Note that the second argument minimum-points-per-cluster will make a cluster with atleast those many points
'''

# Ideally multiple of 8 + 1, But any-number should work
DEFAULT_NUM_POINTS_PER_CLUSTER = 25

def gen_points(k, num_points_per_cluster=DEFAULT_NUM_POINTS_PER_CLUSTER):
    # Distance between clusters
    distance_padding = 100
    points = []
    p = 0
    for _ in range(k):
        local_points = get_points_for_center(p, p, num_points_per_cluster)
        points.extend(local_points)
        p += num_points_per_cluster + distance_padding 
    return points


def get_points_for_center(x, y, num_points):
    points = [(x, y)]
    count = 1
    inc = 1
    while count < num_points:
        points.append((x-inc, y))
        points.append((x+inc, y))
        points.append((x, y+inc))
        points.append((x, y-inc))
        points.append((x+inc, y+inc))
        points.append((x-inc, y-inc))
        points.append((x+inc, y-inc))
        points.append((x-inc, y+inc))
        inc += 1
        count += 8
    return points

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 1:
        print("Usage ./gen_points.py k <(optional) minimum-points-per-cluster>")
    k = int(args[0])

    num_points = None
    kwargs = {}
    if len(args) > 1:
        kwargs["num_points_per_cluster"] = int(args[1])
        num_points = args[1]
    points = gen_points(k, **kwargs)

    # Print according to some format
    for p in points:
        print(f"{p[0]},{p[1]}") 
