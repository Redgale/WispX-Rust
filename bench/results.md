
### download, 1 stream

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1874 MiB/s | 1.01x | 432 ms/GiB |
| baseline server + original client (v2) | 1859 MiB/s | 1.00x | 409 ms/GiB |
| new server + original client (v1) | 2121 MiB/s | 1.14x | 329 ms/GiB |
| new server + original client (v2) | 2293 MiB/s | 1.23x | 344 ms/GiB |
| new server + new client (v2) | 4181 MiB/s | 2.25x | 320 ms/GiB |
| new server + new client (WispX) | 4471 MiB/s | 2.41x | 305 ms/GiB |

### download, 16 streams

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1265 MiB/s | 0.98x | 1145 ms/GiB |
| baseline server + original client (v2) | 1292 MiB/s | 1.00x | 1161 ms/GiB |
| new server + original client (v1) | 4131 MiB/s | 3.20x | 589 ms/GiB |
| new server + original client (v2) | 4116 MiB/s | 3.19x | 594 ms/GiB |
| new server + new client (v2) | 3773 MiB/s | 2.92x | 675 ms/GiB |
| new server + new client (WispX) | 2958 MiB/s | 2.29x | 748 ms/GiB |

### upload 16 KiB writes, 1 stream

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 3234 MiB/s | 1.00x | 536 ms/GiB | **25% of uploaded data lost** |
| baseline server + original client (v2) | 3232 MiB/s | 1.00x | 517 ms/GiB | **23% of uploaded data lost** |
| new server + original client (v1) | 3175 MiB/s | 0.98x | 394 ms/GiB | ok |
| new server + original client (v2) | 3193 MiB/s | 0.99x | 394 ms/GiB | ok |
| new server + new client (v2) | 2943 MiB/s | 0.91x | 379 ms/GiB | ok |
| new server + new client (WispX) | 2573 MiB/s | 0.80x | 320 ms/GiB | ok |

### upload 16 KiB writes, 16 streams

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 3352 MiB/s | 1.01x | 576 ms/GiB | ok |
| baseline server + original client (v2) | 3312 MiB/s | 1.00x | 578 ms/GiB | ok |
| new server + original client (v1) | 3479 MiB/s | 1.05x | 481 ms/GiB | ok |
| new server + original client (v2) | 3349 MiB/s | 1.01x | 482 ms/GiB | ok |
| new server + new client (v2) | 3106 MiB/s | 0.94x | 746 ms/GiB | ok |
| new server + new client (WispX) | 3068 MiB/s | 0.93x | 739 ms/GiB | ok |

### request/response 1 KiB, 64 streams

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit |
|---|---:|---:|---:|---:|
| baseline server + original client (v1) | 136502 ops/s | 1.00x | 0.08 / 9.77 ms | 21 ms/1k ops |
| baseline server + original client (v2) | 136586 ops/s | 1.00x | 0.08 / 9.35 ms | 21 ms/1k ops |
| new server + original client (v1) | 151711 ops/s | 1.11x | 0.09 / 6.29 ms | 14 ms/1k ops |
| new server + original client (v2) | 143256 ops/s | 1.05x | 0.10 / 7.39 ms | 15 ms/1k ops |
| new server + new client (v2) | 180996 ops/s | 1.33x | 0.35 / 0.53 ms | 14 ms/1k ops |
| new server + new client (WispX) | 246884 ops/s | 1.81x | 0.25 / 0.48 ms | 11 ms/1k ops |

### request/response 64 B, 1 stream

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit |
|---|---:|---:|---:|---:|
| baseline server + original client (v1) | 46429 ops/s | 1.01x | 0.02 / 0.03 ms | 9 ms/1k ops |
| baseline server + original client (v2) | 46131 ops/s | 1.00x | 0.02 / 0.03 ms | 9 ms/1k ops |
| new server + original client (v1) | 44001 ops/s | 0.95x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + original client (v2) | 44063 ops/s | 0.96x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + new client (v2) | 43450 ops/s | 0.94x | 0.02 / 0.04 ms | 10 ms/1k ops |
| new server + new client (WispX) | 43845 ops/s | 0.95x | 0.02 / 0.04 ms | 10 ms/1k ops |

### open+request+close, 32 workers

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit | data integrity |
|---|---:|---:|---:|---:|---|
| baseline server + original client (v1) | 35079 ops/s | 1.00x | 0.66 / 6.20 ms | 92 ms/1k ops | **315 truncated responses/s** |
| baseline server + original client (v2) | 35178 ops/s | 1.00x | 0.67 / 6.00 ms | 92 ms/1k ops | **314 truncated responses/s** |
| new server + original client (v1) | 9106 ops/s | 0.26x | 3.77 / 4.30 ms | 67 ms/1k ops | ok |
| new server + original client (v2) | 9132 ops/s | 0.26x | 3.76 / 4.30 ms | 67 ms/1k ops | ok |
| new server + new client (v2) | 80599 ops/s | 2.29x | 0.38 / 0.79 ms | 47 ms/1k ops | ok |
| new server + new client (WispX) | 77112 ops/s | 2.19x | 0.40 / 0.79 ms | 47 ms/1k ops | ok |

### open+request+close, 8 conns x 8 workers

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit | data integrity |
|---|---:|---:|---:|---:|---|
| baseline server + original client (v1) | 0 ops/s | 0.00x | 0.00 / 0.00 ms | 80 ms/1k ops | **2229 truncated responses/s** |
| baseline server + original client (v2) | 53127 ops/s | 1.00x | 1.02 / 1.57 ms | 81 ms/1k ops | **7116 truncated responses/s** |
| new server + original client (v1) | 17702 ops/s | 0.33x | 3.85 / 4.75 ms | 56 ms/1k ops | **0 truncated responses/s** |
| new server + original client (v2) | 17340 ops/s | 0.33x | 3.92 / 4.71 ms | 55 ms/1k ops | **0 truncated responses/s** |
| new server + new client (v2) | 92946 ops/s | 1.75x | 0.66 / 1.54 ms | 45 ms/1k ops | ok |
| new server + new client (WispX) | 93206 ops/s | 1.75x | 0.66 / 1.48 ms | 45 ms/1k ops | ok |

### new connection to first byte, 50 ms RTT

| setup | result | vs baseline v2 |
|---|---:|---:|
| baseline server + original client (v1) | 107.0 ms | 1.49x |
| baseline server + original client (v2) | 159.8 ms | 1.00x |
| new server + original client (v1) | 107.1 ms | 1.49x |
| new server + original client (v2) | 159.5 ms | 1.00x |
| new server + new client (v2) | 159.0 ms | 1.00x |
| new server + new client (WispX) | 106.9 ms | 1.50x |

### upload 16 KiB writes, 1 stream, 50 ms RTT

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 2334 MiB/s | 1.00x | 518 ms/GiB | **7% of uploaded data lost** |
| baseline server + original client (v2) | 2339 MiB/s | 1.00x | 551 ms/GiB | **11% of uploaded data lost** |
| new server + original client (v1) | 2474 MiB/s | 1.06x | 380 ms/GiB | ok |
| new server + original client (v2) | 2472 MiB/s | 1.06x | 359 ms/GiB | ok |
| new server + new client (v2) | 2367 MiB/s | 1.01x | 364 ms/GiB | ok |
| new server + new client (WispX) | 1053 MiB/s | 0.45x | 360 ms/GiB | ok |

### download, 1 stream, 50 ms RTT

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1689 MiB/s | 0.90x | 478 ms/GiB |
| baseline server + original client (v2) | 1874 MiB/s | 1.00x | 448 ms/GiB |
| new server + original client (v1) | 2223 MiB/s | 1.19x | 451 ms/GiB |
| new server + original client (v2) | 2645 MiB/s | 1.41x | 434 ms/GiB |
| new server + new client (v2) | 2454 MiB/s | 1.31x | 479 ms/GiB |
| new server + new client (WispX) | 1000 MiB/s | 0.53x | 409 ms/GiB |
