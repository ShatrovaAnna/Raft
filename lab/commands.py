from abc import abstractmethod, ABC

from state_machine import HashTableStateMachine, StateMachine

#should be realised in all class
class StateMachineCommand(ABC):
    @abstractmethod
    def apply(self, state_machine: StateMachine):
        pass


class HashtableCommand(StateMachineCommand):
    @abstractmethod
    def apply(self, state_machine: HashTableStateMachine):
        pass


class HashtablePost(HashtableCommand):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def apply(self, state_machine: HashTableStateMachine):
        state_machine.add(self.key, self.value)
