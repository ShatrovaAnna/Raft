from hashlib import sha256


class StateMachine:
    def __init__(self, state):
        self.state = state


class HashTableStateMachine(StateMachine):
    def __init__(self, capacity):
        self.capacity = capacity
        super().__init__([dict() for _ in range(capacity)])
    # to encode to byte, hash, to byte, to int
    def get(self, key: str):
        hashed = int.from_bytes(sha256(key.encode()).digest(), "little")
        return self.state[hashed % self.capacity][key]

    def add(self, key: str, value: str):
        hashed = int.from_bytes(sha256(key.encode()).digest(), "little")
        self.state[hashed % self.capacity][key] = value
