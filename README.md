# Topic 5: Thread Synchronization in a Bounded-Buffer System

A producer-consumer program in C++. Several producer threads create tasks and put them into a shared buffer with limited capacity. Several consumer threads take tasks out and process them. One mutex protects the buffer and the counters, and two counting semaphores stop producers when the buffer is full and consumers when it is empty.

Scenario: a small job-processing service. Producers submit jobs, consumers are worker threads that run them. Running a job is simulated with a short sleep.

## Repository contents

| Path | Content |
|---|---|
| `task5.cpp` | Complete source code (single file) |
| `report/Task5_Report.docx`, `report/Task5_Report.pdf` | Technical report |
| `diagrams/sync_diagram.png` | Synchronization diagram (source: `sync_diagram.dot`) |
| `screenshots/` | Terminal screenshots of every menu option |
| `results/` | Raw text output of every run used in the report |

## Dependencies

- A C++ compiler with C++20 support, because `<semaphore>` is a C++20 header. g++ 11 or newer works. Tested with g++ 13.3.0.
- POSIX threads (`-pthread`). Works on Linux, macOS and Windows with WSL.
- No other libraries.

## Build

```
g++ -std=c++20 -pthread task5.cpp -o buffer
```

## Usage

```
./buffer
```

A menu appears:

```
1. Normal run (with logs)
2. Wrong way (no mutex) demo
3. Tests (full, empty, high contention)
4. Experiments
0. Exit
```

| Option | What it does |
|---|---|
| 1 | 3 producers, 2 consumers, capacity 3, 4 tasks each. Prints every step and the task accounting. |
| 2 | 4 threads increment a counter 3,000,000 times each, first without a mutex and then with one. Shows lost updates. |
| 3 | Full-buffer test, empty-buffer test and high-contention test. Each prints PASS or FAIL. |
| 4 | Experiments that change the number of consumers, the buffer capacity and the number of producers, and print throughput. |
| 0 | Exit |

Every run prints Created, Processed, Completed, Duplicate, Lost, wait counts, time and throughput. `RESULT: PASS` means every task was completed exactly once and the buffer was empty at the end.

## Test suite

| Case | Type | Expected |
|---|---|---|
| Normal run (option 1) | Normal | 12/12 tasks, PASS |
| Full buffer, capacity 1, slow consumer (option 3) | Boundary | Producers wait, PASS |
| Empty buffer, 1 producer, 4 consumers (option 3) | Boundary | Consumers wait, PASS |
| High contention, 8 + 8 threads, capacity 1 (option 3) | Stress | 8000/8000 tasks, PASS |
| Counter without mutex (option 2) | Failure scenario | Result below 12,000,000 |

## Limitations

- Task work is simulated with `sleep`, so the experiments show behaviour for waiting-type work (disk, network), not CPU-heavy work.
- The results in this repository were measured on a machine with one CPU core. Times will be different on other machines.
- All parameters are fixed in the code. To try other values, change the `run(...)` calls in `runTests()` and `runExperiments()`.
- One global mutex protects all shared data. This is simple and safe but can become a bottleneck with very many threads.
- The wait counters use `try_acquire()` before `acquire()`, so they count how often a thread found the buffer full or empty, not the exact time spent waiting.
