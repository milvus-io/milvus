from annoy import AnnoyIndex

a = AnnoyIndex(3, 'angular')
a.add_item(0, [1, 0, 0])
a.add_item(1, [0, 1, 0])
a.add_item(2, [0, 0, 1])
a.build(-1)
a.save('test.tree')

b = AnnoyIndex(3)
b.load('test.tree')

print(b.get_nns_by_item(0, 100))
print(b.get_nns_by_vector([1.0, 0.5, 0.5], 100))
