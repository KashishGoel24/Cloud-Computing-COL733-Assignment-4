from __future__ import annotations

import copy
from abc import ABC, abstractmethod
# from bisect import bisect_left, insort
import threading
from typing import Any, Final

from sortedcontainers import SortedList  # type: ignore

from core.message import JsonMessage


class State(ABC):
    pass



class LogicalTime:
  def __init__(self, name: str, ts: int = 0) -> None:
    self.ts: Final[int] = ts
    self.name: Final[str] = name

  def __repr__(self) -> str:
    return f"{self.name}: {self.ts}"

  def __eq__(self, other: object) -> bool:
    if isinstance(other, LogicalTime):
      return self.ts == other.ts and self.name == other.name
    raise ValueError

  def __lt__(self, other: LogicalTime) -> bool:
    if self.ts < other.ts:
      return True
    if self.ts > other.ts:
      return False
    return self.name < other.name

  def serialize(self) -> tuple[str, int]:
    return self.name, self.ts


class LogEntry(ABC):
  def __init__(self, msg: JsonMessage):
    assert "ltime" in msg, "Set ltime before getting"
    self.ltime = LogicalTime(msg["ltime"][1], msg["ltime"][0])

  @abstractmethod
  def do(self, state: State) -> None:
    """
    Apply this log entry on the state.
    """
    pass

  @abstractmethod
  def undo(self, state: State) -> None:
    """
    Undo this log entry on the state.
    """
    pass

  def __eq__(self, other: object) -> bool:
    if isinstance(other, LogEntry):
      return self.ltime == other.ltime
    raise ValueError

  def __lt__(self, other: LogEntry) -> bool:
    return self.ltime < other.ltime

  @abstractmethod
  def to_dict(self) -> dict[str, Any]:
    pass


class VectorTime:
  def __init__(self, times: list[LogicalTime])-> None:
    self._vector: dict[str, LogicalTime] = {t.name: t for t in times}

  @staticmethod
  def new(servers: list[str]) -> VectorTime:
    return VectorTime([LogicalTime(s) for s in servers])

  def __repr__(self) -> str:
    return repr(self._vector)

  def __getitem__(self, server: str) -> LogicalTime:
    return self._vector[server]

  def advance(self, ltime: LogicalTime) -> None:
    s = ltime.name  # server name
    assert self._vector[s].ts <= ltime.ts, \
      f"Tried to take {s} clock backwards from {self._vector[s].ts} to {ltime.ts}!"
    self._vector[s] = ltime

  def is_ltime_earlier(self, ltime: LogicalTime) -> bool:
    s = ltime.name  # server name
    return ltime.ts <= self._vector[s].ts

  def is_vtime_earlier(self, vtime: VectorTime) -> bool:
    # We don't use Python's __lt__ here since vector times do not have a total order.
    # It is possible that (not a < b) and (not b < a)
    for s in vtime._vector:
      if not self.is_ltime_earlier(vtime._vector[s]):
        return False
    return True

  def __eq__(self, other: object) -> bool:
    if isinstance(other, VectorTime):
      for key in self._vector:
        if other._vector[key] != self._vector[key]:
          return False
      return True
    raise ValueError

  def to_dict(self) -> dict[str, float]:
    return {k: t.ts for k, t in self._vector.items()}

class Storage:
  def __init__(self, servers: list[str], init_state: State):
    # Timestamp till which we have committed the writes.
    self.c = VectorTime.new(servers)
    # Timestamp till which we have performed the tentative writes.
    self.f = VectorTime.new(servers)

    self.committed_log: list[LogEntry] = []
    self.tentative_log: SortedList[LogEntry] = SortedList(key=lambda l: l.ltime)

    self.committed_st: State = copy.deepcopy(init_state)
    self.tentative_st: State = copy.deepcopy(init_state)

    self._apply_lock: threading.Lock = threading.Lock()

  def chk_invariants(self) -> None:
    # F should always be ahead of C. C should always be ahead of O.
    # print("inside th check invariant thing", self.c , self.f)
    assert self.f.is_vtime_earlier(self.c)
    # print("the time invariant passed")
    # If tentative log is empty, F should be equal to C; committed state and tentative state
    # should be equivalent
    if len(self.tentative_log) == 0:
      assert self.f == self.c
      assert self.committed_st == self.tentative_st

  def apply(self, commits: list[LogEntry], tentatives: SortedList[LogEntry]) -> None:
    """
    Apply the list of commits and tentatives to the state.
    Args:
      commits: These can be out of order.
      tentatives: These are given in the order of ltime.ts.
    """
    # TODO-3
    # raise NotImplemented()
    # here we have the committed logs in the order we have to apply
    # we also have the tentative logs but those we need to sort with our tentative log first
    # we need to undo to the point the sorted log starts differing from the final sorted log
    # for the committed state we apply the operations in the same order as they reply
    # once undone the tentative state, we replay the tentative logs in the tentative state
    # once done this we update c and f
    with self._apply_lock:

      # name = ''
      # if len(self.committed_log)>0:
      #   name = self.committed_log[0].ltime.name
      # elif len(self.tentative_log)>0:
      #   name = self.tentative_log[0].ltime.name

      for log in reversed(self.tentative_log):
        log.undo(self.tentative_st)

      tentative_logs_removed_commits = SortedList([log for log in self.tentative_log if log not in commits], key=lambda l: l.ltime)
      combined_tentatives = SortedList(tentative_logs_removed_commits, key=lambda l: l.ltime)
      for log in tentatives:
        if log not in combined_tentatives:
          combined_tentatives.add(log)
      # print("the combined tentatives formed are", combined_tentatives)

      
      # here working upon the committed logs
      # print(name,"the initial self.c and self.f are ", self.c, self.f)

      for log in commits:
        if not self.c.is_ltime_earlier(log.ltime):
          log.do(self.committed_st)
          log.do(self.tentative_st)
          self.committed_log.append(log)
          self.c.advance(log.ltime)
          # check if the below thing is right
          # if len(combined_tentatives)==0:
          self.f.advance(log.ltime)

      # print(name,"after update to the committed logs the self.c and self.f are", self.c, self.f)

      # print(name,"the committed log is", self.committed_log)
      # print(name,"the committed state is", self.committed_st)
      # print(name,"tentative state is", self.tentative_st)
      # tentative logs

      # now update the self.tentative log
      self.tentative_log = combined_tentatives
      # print(name,"the tentative log is", self.tentative_log)
      for log in self.tentative_log:
        # if not self.f.is_ltime_earlier(log.ltime):
        log.do(self.tentative_st)
        self.f.advance(log.ltime)

      # print(name,"tentative state finally is", self.tentative_st)
      # print(name,"after update of the tentative logs the self.c and self.f are", self.c, self.f)
      # print(name,"done, now going for checks")
      self.chk_invariants()
      # # print(name,"out")


  def commit(self, writes: list[LogEntry]) -> None:
    """Writes committed writes"""
    # # print("going into the apply function", writes)
    self.apply(writes, SortedList())

  def tentative(self, writes: SortedList[LogEntry]) -> None:
    """Performs tentative writes"""
    self.apply([], writes)

  def anti_entropy(self, c: VectorTime, f: VectorTime) -> tuple[list[LogEntry], SortedList[LogEntry]]:
    """
    Args:
      c: commit vector of the other storage
      f: tentative vector of the other storage

    Returns:
      committed and tentative logEntries that I have and the other storage don't.
    """
    # TODO-4
    # raise NotImplemented()
    committed_logs = []
    tentative_logs = SortedList(key=lambda l: l.ltime)

    if (not self.c.is_vtime_earlier(c)) and (not c.is_vtime_earlier(self.c)):
      raise AssertionError(f"try to get committed log with a commit timestamp that is neither older nor newer")
    
    for log in self.committed_log:
      # print("printing log ltimes", log.ltime)
      if not c.is_ltime_earlier(log.ltime):
        committed_logs.append(log)

    for log in self.tentative_log:
        if not f.is_ltime_earlier(log.ltime): 
            tentative_logs.add(log)

    return committed_logs, tentative_logs