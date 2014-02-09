# KitFS high level structure

## Original idea

Graph based with unidirectional relations. Files have names and are nodes, edges
also have names associated with them.

Nodes: `file1`, `file2`, `file3`

Edges (unidirectional relations):

    file1 --"label1"-> file2
    file2 --"label2"-> file3
    file3 --"label3"-> file2


