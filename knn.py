from sklearn.neighbors import KDTree

points = {
    "A": [-1, -1],
    "B": [-2, -1],
    "C": [-3, -2],
    "D": [1, 1],
    "E": [2, 1],
    "F": [3, 2]
}

items = list(points.items())

X = [item[1] for item in items]
kdt = KDTree(X, leaf_size=30, metric='euclidean')
distances, indices = kdt.query(X, k=2, return_distance=True)

for index, item in enumerate(items):
    nearest_neighbour_index = indices[index][1]
    distance = distances[index][1]
    print(item[0], items[nearest_neighbour_index][0], distance)
