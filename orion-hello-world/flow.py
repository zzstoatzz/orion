from itertools import product
from prefect import task, flow
from prefect.executors import DaskExecutor
from pydantic import BaseModel
from typing import Iterable, Union

class Geometry(BaseModel):
    dimension: int
    width: Union[int, Iterable[int]]

    def size(self: object):
        return self.width**self.dimension
    
class Edge(BaseModel):
    start: int
    end: int

@task 
def possibilities(g: Geometry):
    N = g.size()
    for i,d in product(range(N), range(g.dimension)):
        possibility = i + g.width**d
        print(Edge(start=i, end=possibility % N))

@flow(executor=DaskExecutor())
def main():
    geometry = Geometry(dimension=5, width=4)

    possibilities(geometry)

if __name__ == "__main__":
    main()