# Contributing

This project uses a very minimalistic approach to contributions:

 * Fork it
 * Create a branch
 * Make your changes
 * Commit it with an explanation of [*why* this change is necessary](https://chris.beams.io/posts/git-commit/#why-not-how)
 * Make sure all tests pass
 * Submit a pull request
 * Be patient

## Prerequisites

This project uses Gradle 6.x. A [compatible JDK version](https://docs.gradle.org/current/userguide/compatibility.html#java) is therefore required. 

## Running Tests

To start a local MongoDB node before running tests, use

``` shell
./gradlew startMongoDb
```

Then run the tests:

``` shell
./gradlew check
```
