# distriba
Distributed Data Management - Exercises

## Notes

LargeMessageProxy Excercise is solved using Akka 2.6.0 -> reference.conf in Tests has been modified to conform with new regulations

Kryo currently has a Problem with Illegal reflective access by com.esotericsoftware.kryo.util.UnsafeUtil -> this can be avoided by starting java with the "--illegal-access=deny" option enabled. Functionality seems unaffected for the purpose of the exercise.
