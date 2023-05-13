| # |             Equi-Join arguments             | Execution Time | Tuples | Memory |
|:-:|:-------------------------------------------:|:--------------:|:------:|:------:|
| 1 | f1: D, a1:  3, f2: C, a2: 0, m: 200, J: NLJ |   5.328 sec    |  1997  |  28MB  |
| 2 | f1: D, a1:  3, f2: B, a2: 0, m: 200, J: NLJ |   3.592 sec    |  1126  |  69MB  |
| 3 | f1: A, a1:  3, f2: E, a2: 0, m: 200, J: NLJ |   0.798 sec    |  167   |  29MB  |
| 4 | f1: B, a1:  1, f2: B, a2: 2, m: 200, J: NLJ |   2.026 sec    |  321   |  94MB  |
| 5 | f1: D, a1:  3, f2: C, a2: 0, m: 200, J: SMJ |   1.781 sec    |  1997  |  75MB  |
| 6 | f1: D, a1:  3, f2: B, a2: 0, m: 200, J: SMJ |   1.666 sec    |  1126  |  9MB   |
| 7 | f1: A, a1:  3, f2: E, a2: 0, m: 200, J: SMJ |   5.814 sec    |  167   |  13MB  |
| 8 | f1: B, a1:  1, f2: B, a2: 2, m: 200, J: SMJ |   0.947 sec    |  321   |  49MB  |