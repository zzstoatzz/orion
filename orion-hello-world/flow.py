from itertools import product
from prefect import flow, task
from prefect.executors import DaskExecutor
from pydantic import BaseModel
from secrets import randbelow
from typing import Iterable, List, Union

import coloredlogs
import logging

logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO", logger=logger)

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

    def add_edge(self: object, edge: Edge):
        self.edges.append(edge)

@task
def possibility(i: int, dim: int, width: int) -> Edge:
    return Edge(
        head=Node(index=i), 
        tail=Node(index=i + width**dim)
    )

def shuffle(p: List[Edge]) -> List[Edge]:
    return [p.pop(randbelow(len(p))) for i in range(len(p))]

@task
def elapse(P: List[Edge], N: int) -> Graph:
    G = Graph()
    for event in shuffle(P):
        G.add_edge(event)
    return G

@flow#(executor=DaskExecutor())
def evolve():
    g = Geometry(
        dimension=2, 
        width=2
    )
    N = g.size()

    space = product(range(N), range(g.dimension))
    possibilities = [possibility(i, R, g.width) for i,R in space]
    elapse(possibilities, N=N)

evolve()