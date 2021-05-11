from interpolate import Tree
tree = Tree(3)
tree.insert('123')
tree.insert('123')
tree.insert('123')
tree.insert('000')
tree.insert('222')
tree.insert('876')
tree.insert('999')
tree.insert('654')
tree.insert('645')
# print(tree._nodelist[1]._nodelist[2]._nodelist[3])

for item in tree:
    print(item)

# print(tree._nodelist[1]._nodelist[2]._nodelist[3])
