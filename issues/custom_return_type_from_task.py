from lib2to3.pytree import Base
from prefect import flow, task

from pydantic import BaseModel
from typing import Any


class MyPydanticClass(BaseModel):
    attr: Any
    
@task
def myTask(attr: Any) -> MyPydanticClass:
    print(attr)
    return MyPydanticClass(attr=attr)

@flow
def FlowPassingPydanticObjects():
    pydanticResult = myTask('hi')
    
if __name__ == "__main__":
    FlowPassingPydanticObjects()