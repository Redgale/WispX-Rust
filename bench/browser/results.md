
### HTTP 1 KiB bodies (requests/s, median of 2)

| setup | requests/s | vs today's default | correct responses |
|---|---:|---:|---|
| original server + original epoxy (v1) | 3765 | 1.00x | all 6000 |
| original server + original epoxy (v2) | 3879 | 1.03x | all 6000 |
| new server + original epoxy (v1) | 4086 | 1.09x | all 6000 |
| new server + original epoxy (v2) | 3868 | 1.03x | all 6000 |
| new server + new epoxy (v2) | 3856 | 1.02x | all 6000 |
| new server + new epoxy (WispX) | 3778 | 1.00x | all 6000 |

### HTTP 64 KiB bodies (requests/s, median of 2)

| setup | requests/s | vs today's default | correct responses |
|---|---:|---:|---|
| original server + original epoxy (v1) | 1290 | 1.00x | all 3000 |
| original server + original epoxy (v2) | 1318 | 1.02x | all 3000 |
| new server + original epoxy (v1) | 1644 | 1.27x | all 3000 |
| new server + original epoxy (v2) | 1622 | 1.26x | all 3000 |
| new server + new epoxy (v2) | 1658 | 1.29x | all 3000 |
| new server + new epoxy (WispX) | 1568 | 1.22x | all 3000 |

### HTTPS 1 KiB bodies (requests/s, median of 2)

| setup | requests/s | vs today's default | correct responses |
|---|---:|---:|---|
| original server + original epoxy (v1) | 1132 | 1.00x | all 1200 |
| original server + original epoxy (v2) | 1196 | 1.06x | all 1200 |
| new server + original epoxy (v1) | 1186 | 1.05x | all 1200 |
| new server + original epoxy (v2) | 1273 | 1.12x | all 1200 |
| new server + new epoxy (v2) | 1222 | 1.08x | all 1200 |
| new server + new epoxy (WispX) | 1284 | 1.13x | all 1200 |

### HTTPS 1 MiB bodies (requests/s, median of 2)

| setup | requests/s | vs today's default | correct responses |
|---|---:|---:|---|
| original server + original epoxy (v1) | 32 | 1.00x | all 120 |
| original server + original epoxy (v2) | 32 | 0.97x | all 120 |
| new server + original epoxy (v1) | 35 | 1.08x | all 120 |
| new server + original epoxy (v2) | 36 | 1.09x | all 120 |
| new server + new epoxy (v2) | 34 | 1.06x | all 120 |
| new server + new epoxy (WispX) | 34 | 1.03x | all 120 |
