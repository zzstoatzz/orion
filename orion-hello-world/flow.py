from itertools import product
from prefect import flow, task
from secrets import randbelow
from typing import Iterable, List, Union

class Geometry:
    def __init__(self: object, dimension: int, width: Union[int, Iterable[int]]):
        self.cartesian_size = width**dimension
        self.dimension=dimension
        self.width = width

class Node:
    def __init__(self: object, index: int):
        self.index = index

    def __repr__(self) -> str:
        return f"@{self.index}"

class Edge:
    def __init__(self: object, head: Node, tail: Node):
        self.head = head
        self.tail = tail
        
    def __repr__(self) -> str:
        return f"head: {self.head} tail: {self.tail}"


class Graph:
    def __init__(self: object, edges: Iterable[Edge], nodes: Iterable[Node] = []):
        self.edges = edges
        self.nodes = nodes

graph = Graph([], [])

@flow
def get_possibility(i: int, dim: int, width: int) -> Edge:
    return Edge(
        head=Node(index=i), 
        tail=Node(index=i + width**dim)
    )
    


def shuffle(p: List[Edge]) -> List[Edge]:
    return [p.pop(randbelow(len(p))) for i in range(len(p))]

@flow
def elapse(events: List[Edge]) -> Graph:
    for event in shuffle(events):
        print(event)
        yield event


@flow
def evolve():
    geometry = Geometry(
        dimension=2, 
        width=4
    )
    N = geometry.cartesian_size

    space = product(range(N), range(geometry.dimension))
    
    possibilities = [get_possibility(i, R, geometry.width) for i,R in space]
    
    elapse(possibilities)
    
    
if __name__ == "__main__":
    evolve()