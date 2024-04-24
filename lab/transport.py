from fastapi import Body
from pydantic import BaseModel


class AppendInput(BaseModel):
    term: int = Body(...)
    leader_id: int = Body(...)
    leader_commit: int = Body(...)
    prev_log_index: int = Body(...)
    prev_log_term: int = Body(...)
    entries: list = Body(...)


class AppendOutput(BaseModel):
    term: int = Body(...)
    success: int = Body(...)

#body(...) men that params will get from request-body
class ElectionInput(BaseModel):
    term: int = Body(...)
    candidate_id: int = Body(...)
    last_log_index: int = Body(...)
    last_log_term: int = Body(...)


class ElectionOutput(BaseModel):
    term: int = Body(...)
    vote_granted: bool = Body(...)


class HashtablePostInput(BaseModel):
    key: str = Body(...)
    value: str = Body(...)
