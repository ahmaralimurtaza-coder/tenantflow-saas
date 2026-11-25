| Query | Before (ms) | After (ms) | Speed-up |
|---|---:|---:|---:|
| Q1 latest 50 orders of a tenant | 31.564 | 0.063 | **501.0×** |
| Q2 tenant orders in last 7 days | 19.946 | 2.505 | **8.0×** |

_200,000 rows in `orders`, median of 7 runs, tenant `acme`, 2026-11-25T04:23:56.642Z_
