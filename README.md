# distriba
Distributed Data Management - Exercises

## DDM-LMP

### Notes

LargeMessageProxy Excercise is solved using Akka __2.6.0__ 
-> reference.conf in Tests has been modified to conform with new regulations

Kryo currently has a Problem with Illegal reflective access by com.esotericsoftware.kryo.util.UnsafeUtil 
(https://github.com/EsotericSoftware/kryo/issues/543)

-> this can be avoided by starting java with the "--illegal-access=deny" option enabled. Functionality seems unaffected for the purpose of the exercise.

### Tests

The implementation was tested on serveral machines:
1. Normal Machines:
 __13" MacBook Pro 2017 (8GB Ram i5), a 15" MacBook Pro 2019 (32GB Ram, i9), a desktop machine (24GB Ram i5)__
 --> on these machines remote execution works flawlessly up to 9 workers

 2. Raspberry Pi:
__2 Raspberry Pi 3B+ (1GB Ram, Cortex-A53)__
 -> remote execution works with up to 10 workers (9 each, with both machines as slaves) when JVM memory is extended with "-Xmx1800m" (Swap is configured to 2048 on both machines)
*This is only the tested maximum after which we stopped. Potentially more workes possible!*