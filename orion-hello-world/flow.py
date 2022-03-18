from itertools import product
from prefect import flow, task
from pydantic import BaseModel
from secrets import randbelow
from typing import Iterable, List, Union

class Geometry(BaseModel):
    dimension: int
    width: Union[int, Iterable[int]]

    def size(self: object): # assumed isochoric
        return self.width**self.dimension

class Node(BaseModel):
    index: int

class Edge(BaseModel):
    head: Node
    tail: Node

class Graph(BaseModel):
    edges: Iterable[Edge] = []
    nodes: Iterable[Node] = []

@task
def get_possibility(i: int, dim: int, width: int) -> Edge:
    return Edge(
        head=Node(index=i), 
        tail=Node(index=i + width**dim)
    )


def shuffle(p: List[Edge]) -> List[Edge]:
    return [p.pop(randbelow(len(p))) for i in range(len(p))]

@task
def elapse(events: List[Edge], N: int) -> Graph:
    G = Graph()
    G.edges = [event for event in shuffle(events)]
    return G

@flow
def evolve():
    g = Geometry(
        dimension=2, 
        width=2
    )
    N = g.size()

    space = product(range(N), range(g.dimension))
    possibilities = [get_possibility(i, R, g.width) for i,R in space]
    elapse(possibilities, N=N)
    
if __name__ == "__main__":
    evolve()