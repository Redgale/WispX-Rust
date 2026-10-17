
### download, 1 stream

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 2122 MiB/s | 1.14x | 389 ms/GiB |
| baseline server + original client (v2) | 1857 MiB/s | 1.00x | 437 ms/GiB |
| new server + original client (v1) | 2089 MiB/s | 1.12x | 325 ms/GiB |
| new server + original client (v2) | 2069 MiB/s | 1.11x | 329 ms/GiB |
| new server + new client (v2) | 4526 MiB/s | 2.44x | 308 ms/GiB |
| new server + new client (WispX) | 4054 MiB/s | 2.18x | 316 ms/GiB |

### download, 16 streams

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1281 MiB/s | 1.01x | 1142 ms/GiB |
| baseline server + original client (v2) | 1269 MiB/s | 1.00x | 1140 ms/GiB |
| new server + original client (v1) | 4145 MiB/s | 3.27x | 590 ms/GiB |
| new server + original client (v2) | 4135 MiB/s | 3.26x | 591 ms/GiB |
| new server + new client (v2) | 3803 MiB/s | 3.00x | 667 ms/GiB |
| new server + new client (WispX) | 3036 MiB/s | 2.39x | 743 ms/GiB |

### upload 16 KiB writes, 1 stream

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 3261 MiB/s | 0.96x | 518 ms/GiB | **23% of uploaded data lost** |
| baseline server + original client (v2) | 3392 MiB/s | 1.00x | 512 ms/GiB | **22% of uploaded data lost** |
| new server + original client (v1) | 3113 MiB/s | 0.92x | 389 ms/GiB | ok |
| new server + original client (v2) | 3169 MiB/s | 0.93x | 395 ms/GiB | ok |
| new server + new client (v2) | 2941 MiB/s | 0.87x | 386 ms/GiB | ok |
| new server + new client (WispX) | 3853 MiB/s | 1.14x | 222 ms/GiB | ok |

### upload 16 KiB writes, 16 streams

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 3331 MiB/s | 0.99x | 579 ms/GiB | ok |
| baseline server + original client (v2) | 3353 MiB/s | 1.00x | 578 ms/GiB | ok |
| new server + original client (v1) | 3663 MiB/s | 1.09x | 485 ms/GiB | ok |
| new server + original client (v2) | 2243 MiB/s | 0.67x | 466 ms/GiB | ok |
| new server + new client (v2) | 3080 MiB/s | 0.92x | 751 ms/GiB | ok |
| new server + new client (WispX) | 3638 MiB/s | 1.09x | 378 ms/GiB | ok |

### request/response 1 KiB, 64 streams

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit |
|---|---:|---:|---:|---:|
| baseline server + original client (v1) | 135474 ops/s | 1.01x | 0.08 / 9.08 ms | 21 ms/1k ops |
| baseline server + original client (v2) | 134601 ops/s | 1.00x | 0.08 / 10.04 ms | 21 ms/1k ops |
| new server + original client (v1) | 152360 ops/s | 1.13x | 0.09 / 6.35 ms | 14 ms/1k ops |
| new server + original client (v2) | 152843 ops/s | 1.14x | 0.09 / 6.55 ms | 14 ms/1k ops |
| new server + new client (v2) | 183315 ops/s | 1.36x | 0.35 / 0.49 ms | 13 ms/1k ops |
| new server + new client (WispX) | 204655 ops/s | 1.52x | 0.31 / 0.49 ms | 12 ms/1k ops |

### request/response 64 B, 1 stream

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit |
|---|---:|---:|---:|---:|
| baseline server + original client (v1) | 46143 ops/s | 1.00x | 0.02 / 0.03 ms | 9 ms/1k ops |
| baseline server + original client (v2) | 46151 ops/s | 1.00x | 0.02 / 0.03 ms | 9 ms/1k ops |
| new server + original client (v1) | 44004 ops/s | 0.95x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + original client (v2) | 44623 ops/s | 0.97x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + new client (v2) | 44099 ops/s | 0.96x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + new client (WispX) | 44520 ops/s | 0.96x | 0.02 / 0.03 ms | 10 ms/1k ops |

### open+request+close, 32 workers

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit | data integrity |
|---|---:|---:|---:|---:|---|
| baseline server + original client (v1) | 34974 ops/s | 1.00x | 0.66 / 6.27 ms | 92 ms/1k ops | **261 truncated responses/s** |
| baseline server + original client (v2) | 34963 ops/s | 1.00x | 0.66 / 6.23 ms | 92 ms/1k ops | **258 truncated responses/s** |
| new server + original client (v1) | 9123 ops/s | 0.26x | 3.76 / 4.28 ms | 68 ms/1k ops | ok |
| new server + original client (v2) | 9123 ops/s | 0.26x | 3.77 / 4.27 ms | 67 ms/1k ops | ok |
| new server + new client (v2) | 81137 ops/s | 2.32x | 0.38 / 0.76 ms | 47 ms/1k ops | ok |
| new server + new client (WispX) | 73717 ops/s | 2.11x | 0.42 / 0.77 ms | 48 ms/1k ops | ok |

### open+request+close, 8 conns x 8 workers

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit | data integrity |
|---|---:|---:|---:|---:|---|
| baseline server + original client (v1) | 55007 ops/s | 1.01x | 1.02 / 1.55 ms | 79 ms/1k ops | **3703 truncated responses/s** |
| baseline server + original client (v2) | 54435 ops/s | 1.00x | 1.03 / 1.55 ms | 80 ms/1k ops | **6424 truncated responses/s** |
| new server + original client (v1) | 16573 ops/s | 0.30x | 3.97 / 4.33 ms | 53 ms/1k ops | ok |
| new server + original client (v2) | 16467 ops/s | 0.30x | 3.98 / 4.33 ms | 53 ms/1k ops | ok |
| new server + new client (v2) | 92952 ops/s | 1.71x | 0.66 / 1.53 ms | 45 ms/1k ops | ok |
| new server + new client (WispX) | 90647 ops/s | 1.67x | 0.67 / 1.56 ms | 46 ms/1k ops | ok |

### new connection to first byte, 50 ms RTT

| setup | result | vs baseline v2 |
|---|---:|---:|
| baseline server + original client (v1) | 106.3 ms | 1.50x |
| baseline server + original client (v2) | 159.4 ms | 1.00x |
| new server + original client (v1) | 106.9 ms | 1.49x |
| new server + original client (v2) | 159.3 ms | 1.00x |
| new server + new client (v2) | 159.8 ms | 1.00x |
| new server + new client (WispX) | 106.8 ms | 1.49x |

### upload 16 KiB writes, 1 stream, 50 ms RTT

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 2398 MiB/s | 0.98x | 543 ms/GiB | **9% of uploaded data lost** |
| baseline server + original client (v2) | 2435 MiB/s | 1.00x | 548 ms/GiB | **10% of uploaded data lost** |
| new server + original client (v1) | 2440 MiB/s | 1.00x | 361 ms/GiB | ok |
| new server + original client (v2) | 2499 MiB/s | 1.03x | 374 ms/GiB | ok |
| new server + new client (v2) | 2306 MiB/s | 0.95x | 376 ms/GiB | ok |
| new server + new client (WispX) | 2747 MiB/s | 1.13x | 308 ms/GiB | ok |

### download, 1 stream, 50 ms RTT

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1732 MiB/s | 1.02x | 469 ms/GiB |
| baseline server + original client (v2) | 1702 MiB/s | 1.00x | 485 ms/GiB |
| new server + original client (v1) | 2736 MiB/s | 1.61x | 475 ms/GiB |
| new server + original client (v2) | 2652 MiB/s | 1.56x | 448 ms/GiB |
| new server + new client (v2) | 2542 MiB/s | 1.49x | 489 ms/GiB |
| new server + new client (WispX) | 2630 MiB/s | 1.55x | 492 ms/GiB |
