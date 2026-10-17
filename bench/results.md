
### download, 1 stream

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1721 MiB/s | 0.73x | 440 ms/GiB |
| baseline server + original client (v2) | 2358 MiB/s | 1.00x | 347 ms/GiB |
| new server + original client (v1) | 1873 MiB/s | 0.79x | 391 ms/GiB |
| new server + original client (v2) | 2659 MiB/s | 1.13x | 370 ms/GiB |
| new server + new client (v2) | 3495 MiB/s | 1.48x | 364 ms/GiB |
| new server + new client (WispX) | 2857 MiB/s | 1.21x | 417 ms/GiB |

### download, 16 streams

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1282 MiB/s | 1.01x | 1131 ms/GiB |
| baseline server + original client (v2) | 1271 MiB/s | 1.00x | 1138 ms/GiB |
| new server + original client (v1) | 3490 MiB/s | 2.75x | 692 ms/GiB |
| new server + original client (v2) | 3425 MiB/s | 2.69x | 696 ms/GiB |
| new server + new client (v2) | 3221 MiB/s | 2.53x | 769 ms/GiB |
| new server + new client (WispX) | 3010 MiB/s | 2.37x | 789 ms/GiB |

### upload 16 KiB writes, 1 stream

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 3389 MiB/s | 1.08x | 529 ms/GiB | **19% of uploaded data lost** |
| baseline server + original client (v2) | 3135 MiB/s | 1.00x | 488 ms/GiB | **17% of uploaded data lost** |
| new server + original client (v1) | 3175 MiB/s | 1.01x | 396 ms/GiB | ok |
| new server + original client (v2) | 3152 MiB/s | 1.01x | 381 ms/GiB | ok |
| new server + new client (v2) | 2797 MiB/s | 0.89x | 340 ms/GiB | ok |
| new server + new client (WispX) | 3897 MiB/s | 1.24x | 225 ms/GiB | ok |

### upload 16 KiB writes, 16 streams

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 3364 MiB/s | 1.01x | 575 ms/GiB | ok |
| baseline server + original client (v2) | 3342 MiB/s | 1.00x | 577 ms/GiB | ok |
| new server + original client (v1) | 3691 MiB/s | 1.10x | 484 ms/GiB | ok |
| new server + original client (v2) | 3571 MiB/s | 1.07x | 486 ms/GiB | ok |
| new server + new client (v2) | 3014 MiB/s | 0.90x | 747 ms/GiB | ok |
| new server + new client (WispX) | 3695 MiB/s | 1.11x | 390 ms/GiB | ok |

### request/response 1 KiB, 64 streams

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit |
|---|---:|---:|---:|---:|
| baseline server + original client (v1) | 134788 ops/s | 1.00x | 0.08 / 9.14 ms | 21 ms/1k ops |
| baseline server + original client (v2) | 135310 ops/s | 1.00x | 0.08 / 8.80 ms | 21 ms/1k ops |
| new server + original client (v1) | 152269 ops/s | 1.13x | 0.09 / 6.40 ms | 14 ms/1k ops |
| new server + original client (v2) | 151324 ops/s | 1.12x | 0.09 / 6.33 ms | 14 ms/1k ops |
| new server + new client (v2) | 188330 ops/s | 1.39x | 0.34 / 0.50 ms | 13 ms/1k ops |
| new server + new client (WispX) | 212751 ops/s | 1.57x | 0.30 / 0.47 ms | 12 ms/1k ops |

### request/response 64 B, 1 stream

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit |
|---|---:|---:|---:|---:|
| baseline server + original client (v1) | 46048 ops/s | 0.99x | 0.02 / 0.03 ms | 9 ms/1k ops |
| baseline server + original client (v2) | 46331 ops/s | 1.00x | 0.02 / 0.03 ms | 9 ms/1k ops |
| new server + original client (v1) | 44263 ops/s | 0.96x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + original client (v2) | 44438 ops/s | 0.96x | 0.02 / 0.04 ms | 10 ms/1k ops |
| new server + new client (v2) | 44557 ops/s | 0.96x | 0.02 / 0.03 ms | 10 ms/1k ops |
| new server + new client (WispX) | 44336 ops/s | 0.96x | 0.02 / 0.03 ms | 10 ms/1k ops |

### open+request+close, 32 workers

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit | data integrity |
|---|---:|---:|---:|---:|---|
| baseline server + original client (v1) | 34988 ops/s | 1.00x | 0.66 / 6.22 ms | 92 ms/1k ops | **269 truncated responses/s** |
| baseline server + original client (v2) | 34970 ops/s | 1.00x | 0.65 / 6.40 ms | 92 ms/1k ops | **250 truncated responses/s** |
| new server + original client (v1) | 1453 ops/s | 0.04x | 21.97 / 23.47 ms | 244 ms/1k ops | ok |
| new server + original client (v2) | 1450 ops/s | 0.04x | 21.99 / 23.40 ms | 244 ms/1k ops | ok |
| new server + new client (v2) | 1462 ops/s | 0.04x | 21.94 / 23.24 ms | 219 ms/1k ops | ok |
| new server + new client (WispX) | 74776 ops/s | 2.14x | 0.42 / 0.80 ms | 47 ms/1k ops | ok |

### open+request+close, 8 conns x 8 workers

| setup | result | vs baseline v2 | p50 / p99 latency | server CPU per unit | data integrity |
|---|---:|---:|---:|---:|---|
| baseline server + original client (v1) | 53919 ops/s | 1.00x | 1.01 / 1.54 ms | 81 ms/1k ops | **7131 truncated responses/s** |
| baseline server + original client (v2) | 54161 ops/s | 1.00x | 1.01 / 1.53 ms | 81 ms/1k ops | **7242 truncated responses/s** |
| new server + original client (v1) | 1073 ops/s | 0.02x | 21.61 / 23.52 ms | 202 ms/1k ops | ok |
| new server + original client (v2) | 2902 ops/s | 0.05x | 22.02 / 24.09 ms | 217 ms/1k ops | ok |
| new server + new client (v2) | 2953 ops/s | 0.05x | 21.64 / 23.20 ms | 196 ms/1k ops | ok |
| new server + new client (WispX) | 88266 ops/s | 1.63x | 0.69 / 1.53 ms | 47 ms/1k ops | ok |

### new connection to first byte, 50 ms RTT

| setup | result | vs baseline v2 |
|---|---:|---:|
| baseline server + original client (v1) | 107.1 ms | 1.49x |
| baseline server + original client (v2) | 159.7 ms | 1.00x |
| new server + original client (v1) | 107.2 ms | 1.49x |
| new server + original client (v2) | 159.6 ms | 1.00x |
| new server + new client (v2) | 159.7 ms | 1.00x |
| new server + new client (WispX) | 106.8 ms | 1.49x |

### upload 16 KiB writes, 1 stream, 50 ms RTT

| setup | result | vs baseline v2 | server CPU per unit | data integrity |
|---|---:|---:|---:|---|
| baseline server + original client (v1) | 2338 MiB/s | 0.99x | 533 ms/GiB | **12% of uploaded data lost** |
| baseline server + original client (v2) | 2365 MiB/s | 1.00x | 552 ms/GiB | **11% of uploaded data lost** |
| new server + original client (v1) | 2451 MiB/s | 1.04x | 363 ms/GiB | ok |
| new server + original client (v2) | 2418 MiB/s | 1.02x | 351 ms/GiB | ok |
| new server + new client (v2) | 2071 MiB/s | 0.88x | 361 ms/GiB | ok |
| new server + new client (WispX) | 270 MiB/s | 0.11x | 849 ms/GiB | **0% of uploaded data lost** |

### download, 1 stream, 50 ms RTT

| setup | result | vs baseline v2 | server CPU per unit |
|---|---:|---:|---:|
| baseline server + original client (v1) | 1929 MiB/s | 1.04x | 419 ms/GiB |
| baseline server + original client (v2) | 1848 MiB/s | 1.00x | 430 ms/GiB |
| new server + original client (v1) | 2590 MiB/s | 1.40x | 513 ms/GiB |
| new server + original client (v2) | 2456 MiB/s | 1.33x | 522 ms/GiB |
| new server + new client (v2) | 2538 MiB/s | 1.37x | 515 ms/GiB |
| new server + new client (WispX) | 268 MiB/s | 0.14x | 1025 ms/GiB |
