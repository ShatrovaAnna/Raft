from enum import Enum

from pydantic import BaseModel


class RaftRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class ProcessedElectionResults(BaseModel):
    positive_responses: int = 0
    max_term: int = -1


class CommandLineArguments(BaseModel):
    ip: str
    port: int
    other_ports: list[int]
