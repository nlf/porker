### Testing

In order to run the tests for Porker, you should create a database that will be used _only_ for testing purposes. By default the test suite expects this database to be named `porker_test_suite` and that the currently logged in user can connect to the database without passing a hostname, port or any further credentials. If you need to override this default behavior you may export an environment variable `PORKER_CONNECTION` as a full Postgres connection string (i.e. `PORKER_CONNECTION=postgres://user:password@host:port/database`).

THE TESTS ARE DESTRUCTIVE. I REPEAT, DO NOT RUN THE TESTS AGAINST A REAL DATABASE, YOU _WILL_ LOSE DATA.

Before submitting a pull request, please make sure that you have added a test covering your new functionality or bug fix.

### Code style

Porker follows the [hapi style guide](https://github.com/hapijs/contrib/blob/master/Style.md). The test suite will catch most major deviations from the guide so make sure to run tests before submitting a pull request.
